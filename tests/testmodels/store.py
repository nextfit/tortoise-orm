
from tortoise import fields, models


class Vendor(models.Model):
    id = fields.IntegerField(primary_key=True)
    name = fields.CharField(max_length=64)


class Image(models.Model):
    class Meta:
        db_table = "store_image"
        ordering = ['id', ]

    id = fields.IntegerField(primary_key=True)
    src = fields.CharField(max_length=255)


class Brand(models.Model):
    class Meta:
        db_table = "store_brand"
        ordering = ['id', ]

    id = fields.IntegerField(primary_key=True)
    name = fields.CharField(max_length=255)
    image = fields.ForeignKey('models.Image', on_delete=fields.CASCADE, null=True)

    def __str__(self):
        return self.name


class Category(models.Model):
    class Meta:
        db_table = "store_category"
        ordering = ['id', ]

    id = fields.IntegerField(primary_key=True)
    name = fields.CharField(max_length=255)
    image = fields.ForeignKey('models.Image', on_delete=fields.CASCADE, null=True)

    def __str__(self):
        return self.name


class Product(models.Model):
    class Meta:
        db_table = "store_product"
        ordering = ['id', ]

    id = fields.IntegerField(primary_key=True)
    name = fields.CharField(max_length=255)
    price = fields.CharField(max_length=16)

    images = fields.ManyToManyField('models.Image', through='models.ProductImage',)

    categories = fields.ManyToManyField('models.Category',
        through='models.ProductCategory',
        related_name='products'
    )

    brand = fields.ForeignKey('models.Brand',
        on_delete=fields.CASCADE,
        null=True,
        default=None,
        related_name='products'
    )

    vendor = fields.ForeignKey('models.Vendor',
        on_delete=fields.CASCADE,
        null=True,
        default=None,
        related_name='products'
    )

    def __str__(self):
        return self.name


class ProductImage(models.Model):
    class Meta:
        db_table = "store_productimage"
        unique_together = ('product', 'image')

    product = fields.ForeignKey('models.Product', on_delete=fields.CASCADE)
    image = fields.ForeignKey('models.Image', on_delete=fields.CASCADE)


class ProductCategory(models.Model):
    class Meta:
        db_table = "store_productcategory"
        unique_together = ('product', 'category')

    product = fields.ForeignKey('models.Product', on_delete=fields.CASCADE)
    category = fields.ForeignKey('models.Category', on_delete=fields.CASCADE)


async def create_store_objects() -> None:
    import asyncio

    images = [Image(src='brand_image_{}'.format(num)) for num in range(1, 7)]
    await asyncio.gather(*[img.save() for img in images])

    brands = [Brand(name='brand_{}'.format(num), image=images[num-1]) for num in range(1, 7)]
    await asyncio.gather(*[b.save() for b in brands])

    products = [Product(name='product_{:02d}'.format(num), price='$1') for num in range(1, 22)]
    brand_k, counter = 0, 0
    for p in products:
        p.brand = brands[brand_k]
        counter += 1
        if counter >= brand_k + 1:
            brand_k += 1
            counter = 0

    vendors = [Vendor(name='vendor_{}'.format(num)) for num in range(1, 4)]
    await asyncio.gather(*[vendor.save() for vendor in vendors])

    for i, p in enumerate(products):
        p.vendor = vendors[i % 3]

    # vendor_   1  2  3  1  2  3  1  2  3   1   2   3   1   2   3   1   2   3   1   2   3
    # product_  1  2  3  4  5  6  7  8  9  10  11  12  13  14  15  16  17  18  19  20  21

    await asyncio.gather(*[p.save() for p in products])

    # brand_    1  2  2  3  3  3  4  4  4   4   5   5   5   5   5   6   6   6   6   6   6
    # product_  1  2  3  4  5  6  7  8  9  10  11  12  13  14  15  16  17  18  19  20  21

    images = [Image(src='image_{}'.format(num)) for num in range(1, 22)]
    await asyncio.gather(*[img.save() for img in images])

    index, count = 0, 1
    for p in products[5::-1]:
        await p.images.add(*images[index:index+count])
        index, count = index+count, count+1

    # image_    1  2  3  4  5  6  7  8  9  10  11  12  13  14  15  16  17  18  19  20  21
    # product_  6  5  5  4  4  4  3  3  3   3   2   2   2   2   2   1   1   1   1   1   1
    primes = [2, 3, 5, 7]

    images = {num: Image(src='category_image_{}'.format(num)) for num in primes}
    await asyncio.gather(*[img.save() for img in images.values()])

    categories = {num: Category(name='category_{}'.format(num), image=images[num]) for num in primes}
    await asyncio.gather(*[b.save() for b in categories.values()])

    for index, p in enumerate(products):
        await p.categories.add(*[categories[num] for num in primes if (index+1) % num == 0])

    # product_  1  2  3  4  5  6  7  8  9  10  11  12  13  14  15  16  17  18  19  20  21
    # category_    2  3  2  5  2  7  2  3   2       2       2   3   2       2       2   3
    #                          3            5       3       7   5           3       5   7
