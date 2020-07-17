from multiprocessing import Pool
from recsys.data_generator.accumulators import get_accumulators, logger, group_accumulators

class FeatureGenerator:
    def __init__(self, limit, accumulators, save_only_features=False, input_df=None):
        self.limit = limit
        self.accumulators = accumulators
        self.accs_by_action_type = group_accumulators(accumulators)
        self.save_only_features = save_only_features
        self.input = self.preprocess(input_df)
        print("Number of accumulators %d" % len(self.accumulators))


    def preprocess(self, row):
        row["timestamp"] = row["timestamp"].astype(int)
        row["fake_impressions_raw"] = row["fake_impressions"]
        row["fake_impressions"] = row["fake_impressions"].map(lambda x: x.split("|"))
        row["fake_index_interacted"] = row.apply(
            lambda x: x["fake_impressions"].index(x["reference"]) 
            if x["reference"] in x["fake_impressions"] 
            else -1000, axis=1)
        
        return row

    def calculate_features_per_item(self, clickout_id, item_id, price, rank, row):
        obs = row.copy()
        obs["item_id"] = item_id
        obs["item_id_clicked"] = row["reference"]
        obs["was_clicked"] = int(row["reference"] == item_id)
        obs["clickout_id"] = clickout_id
        obs["rank"] = rank
        obs["price"] = price
        obs["current_filters"] = row["current_filters"]
        obs["clickout_step_rev"] = row["clickout_step_rev"]
        obs["clickout_step"] = row["clickout_step"]
        obs["clickout_max_step"] = row["clickout_max_step"]
        self.update_obs_with_acc(obs, row)
        del obs["fake_impressions"]
        del obs["fake_impressions_raw"]
        del obs["fake_prices"]
        del obs["impressions"]
        del obs["impressions_hash"]
        del obs["impressions_raw"]
        del obs["prices"]
        del obs["action_type"]
        return obs
    

    def update_obs_with_acc(self, obs, row):
        features = []
        for acc in self.accumulators:
            value = acc.get_stats(row, obs)
            if hasattr(value, "items"):
                for k, v in value.items():
                    obs[k] = v
                    features.append(k)
            else:
                obs[acc.name] = value

    def generate_features(self):
        rows_gen = self.read_rows()
        output_obs_gen = self.process_rows(rows_gen)
        return self.save_rows(output_obs_gen)

    def save_rows(self, output_obs):
        return [obs for obs in output_obs]


    def read_rows(self):
        dr = self.input.iterrows()
        print("Reading rows")
        for i, row in dr:
            yield row

    def process_rows(self, rows):
        for clickout_id, row in enumerate(rows):            
            if row["action_type"] == "clickout item":
                row["impressions_raw"] = row["impressions"]
                row["impressions"] = row["impressions"].split("|")
                row["impressions_hash"] = "|".join(sorted(row["impressions"]))
                row["index_clicked"] = (
                    row["impressions"].index(row["reference"]) if row["reference"] in row["impressions"] else -1000
                )
                row["prices"] = list(map(int, row["prices"].split("|")))
                row["price_clicked"] = row["prices"][row["index_clicked"]] if row["index_clicked"] >= 0 else 0
                for rank, (item_id, price) in enumerate(zip(row["impressions"], row["prices"])):
                    obs = self.calculate_features_per_item(clickout_id, item_id, price, rank, row)
                    yield obs
            
            if int(row["is_test"]) == 0:
                for acc in self.accs_by_action_type[row["action_type"]]:
                    acc.update_acc(row)