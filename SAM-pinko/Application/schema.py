
def generate_image_list(row):
    """
    Generate a list of image urls to be returned for the client to display in
    the order they are to be displayed in

    :return list:
    """
    images = [row.Product_Image_Url, ]
    if ',' in row.Additional_Image_Urls:
        images = images + row.Additional_Image_Urls.split(',')
    elif len(row.Additional_Image_Urls) > 0:
        images.append(row.Additional_Image_Urls)
    return images


def stringify_price(price):
    """
    Takes any number as input for price and returns it in a normalised format
    which always includes two decimal places ($0.00) for the price

    :return str:
    """
    price = str(float(price))
    if '.' in price and len(price.split('.')[-1]) == 1:
        price = price + '0'
    elif '.' not in price:
        price = price + '.00'
    return price

def stringify_size(size):
    size = str(size)
    if '/' in size: 
        arr = size.split('/')
        for element in arr:
            if "US" in element:
                size = element

    return size.replace('XX Large', 'XXL')\
        .replace('X Large', 'XL')\
        .replace('Large', 'L')\
        .replace('Medium', 'M')\
        .replace('Small', 'S')\
        .replace('X Small', 'XS')\
        .replace('XX Small', 'XXS')


def generate_size_list(row, sizes_response):
    ammended = stringify_size(row.Size)
    if ammended not in sizes_response:
        sizes_response.append(ammended)
    return sizes_response


def generate_price_list(row):
    """
    Generate a list of two elements, original price & current price of product
    :return list:
    """
    return [stringify_price(row.Price), stringify_price(row.Sale_Price)]


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
        'MPN',
        'Category',
        'Condition',
        'Age_Group',
        'Colors',
        'size',
        'Size_system',
        'Gender',
        'Season',
        'Item_Group_ID'
        # 'UPC',
        # 'Color',
        # 'Size',
        # 'Brand',
        # 'Site_Product_Type',
        # 'Title',
        # 'Cleansed_Long_Description',
        # 'Price',
        # 'Sale_Price',
        # 'Product_Url',
        # 'Swatch_Url',
        # 'Product_Image_Url',
        # 'Additional_Image_Urls',
        # 'Availability'
    ]


def schema_to_json(upcs):
    """
    Convert the file schema into a JSON response object to be received by the client

    :param upcs list: contains objects with upc based product data
    :return resp dict: used as a JSON response
    """
    resp = {
        'name': upcs[0].Title,
        'brand': upcs[0].Brand,
        'url': upcs[0].Url,
        'defaultColor': upcs[0].Colors,
        'defaultPrice': [stringify_price(upcs[0].Price), stringify_price(upcs[0].Sale_Price)],
        'Image_Link' : upcs [0].Image_url, 
        'sizes': [],
        'saleprice':upcs[0].Saleprice, 
        'availability': {},
        'description': upcs[0].Description,
        'ecommerce_exclusive': upcs[0].Ecommerce_exclusive,
        'mpn': upcs[0].MPN,
        'category': upcs[0].Category,
        'condition': upcs[0].Condition,
        'Size_system': upcs[0].Size_system,
        'Gender': upcs[0].Gender,
        'Season': upcs[0].Season,
        'Item_Group_ID' : upcs[0].Item_Group_ID
    }
    # resp = {
    #     'name': upcs[0].Title,
    #     'brand': upcs[0].Brand,
    #     'url': upcs[0].Product_Url,
    #     'defaultColor': upcs[0].Color,
    #     'defaultPrice': [stringify_price(upcs[0].Price), stringify_price(upcs[0].Sale_Price)],
    #     'colors': {},
    #     'sizes': [],
    #     'images': {},
    #     'prices': {},
    #     'upcs': {},
    #     'availability': {},
    #     'description': upcs[0].Cleansed_Long_Description,
    #     'details': ""
    # }

    for row in upcs:
        # UPC_KEY = f"{row.Color}/{stringify_size(row.Size)}"
        # resp["colors"][row.Color] = str(row.Swatch_Url)
        resp["sizes"] = generate_size_list(row, resp["sizes"])
        # resp["images"][row.Color] = generate_image_list(row)
        # resp["prices"][UPC_KEY] = generate_price_list(row)
        # resp["upcs"][UPC_KEY] = row.UPC
        resp["availability"][UPC_KEY] = True if row.Availability == "Available" else False

    return {
        "status": "OK",
        "data": resp
    }


