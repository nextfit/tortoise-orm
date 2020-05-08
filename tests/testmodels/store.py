
from tortoise import models, fields


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
        ordering = ['date_modified', ]

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
        verbose_name_plural = 'Product categories'
        unique_together = ('product', 'category')

    product = fields.ForeignKey('models.Product', on_delete=fields.CASCADE)
    category = fields.ForeignKey('models.Category', on_delete=fields.CASCADE)

