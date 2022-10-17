def get_schema():
    """
    Returns the schema for the csv file in-which to retrieve data from
    :return list:
    """
    return [
        'Product_ID',
        'Title',
        'Description',
        'Url',
        'Image_url',
        'Availability',
        'Price',
        'Sale_Price',
        'Ecommerce_exclusive',
        'Brand',
        'Mpn',
        'Category',
        'Condition',
        'Age_Group',
        'Colors',
        'Size',
        'Size_system',
        'Gender',
        'Season',
        'Item_Group_ID'
    ]


def schema_to_json(upcs):
    """
    Convert the file schema into a JSON response object to be received by the client

    :param upcs list: contains objects
    """
    resp = {
        'name': upcs[0].Title,
        'brand': upcs[0].Brand,
        'url': upcs[0].Url,
        'defaultColor': upcs[0].Colors,
        'defaultPrice': upcs[0].Price, 
        'Image_Link' : upcs[0].Image_url, 
        'sizes': {},
        'saleprice':upcs[0].Sale_Price, 
        'availability': {},
        'description': upcs[0].Description,
        'ecommerce_exclusive': upcs[0].Ecommerce_exclusive,
        'mpn': {},
        'category': upcs[0].Category,
        'condition': upcs[0].Condition,
        'Size_system': upcs[0].Size_system,
        'Gender': upcs[0].Gender,
        'Season': upcs[0].Season,
        'Item_Group_ID' : upcs[0].Item_Group_ID,
        'age_group' : upcs[0].Age_Group
    }
  
    for row in upcs:
          resp["sizes"][row.Size] = row.Size  
          resp["mpn"][row.Mpn] = row.Mpn
          resp["availability"][row.Mpn] = True if row.Availability == "in stock" else False

    return {
        "status": "OK",
        "data": resp
    }


