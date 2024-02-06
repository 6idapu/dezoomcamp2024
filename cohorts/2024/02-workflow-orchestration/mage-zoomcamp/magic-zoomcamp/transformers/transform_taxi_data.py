if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    print("Preprocessing: rows with 0 passengers or 0 trip distance: ", data['passenger_count'].isin([0]).sum())
    print("Preprocessing: rows with 0 passengers or 0 trip distance: ", data['trip_distance'].isin([0]).sum())

    #data = data[data['passenger_count'] > 0]
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    data.columns = (data.columns
                   .str.replace('ID','_id')
                    .str.lower()
    )

    
    return data[(data['passenger_count'] > 0) & (data['trip_distance'] > 0)]

