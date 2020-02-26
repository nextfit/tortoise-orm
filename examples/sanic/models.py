from tortoise import Model, fields


class Users(Model):
    id = fields.IntegerField(primary_key=True)
    name = fields.CharField(50)

    def __str__(self):
        return f"User {self.id}: {self.name}"
