echo "0"
# python join_datasets.py
echo "1"
python convert_item_metadata_to_sets.py
echo "2"
python extract_hotel_dense_features.py
echo "3"
python extract_item_prices.py
echo "4"
python extract_item_prices_rank.py
echo "5"
python generate_click_indices.py
echo "6"
python assign_poi_to_items.py
echo "7"
python extract_city_prices_percentiles.py
echo "8"
python extract_item_rating.py