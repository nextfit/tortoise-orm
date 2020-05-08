
from tortoise import fields
from tortoise.models import Model


class NoID(Model):
    name = fields.CharField(max_length=255, null=True)
    desc = fields.TextField(null=True)


class CommentModel(Model):
    class Meta:
        db_table = "comments"
        table_description = "Test Table comment"

    id = fields.IntegerField(primary_key=True, description="Primary key \r*/'`/*\n field for the comments")
    message = fields.TextField(description="Comment messages entered in the blog post")
    rating = fields.IntegerField(description="Upvotes done on the comment")
    escaped_comment_field = fields.TextField(description="This column acts as it's own comment")
    multiline_comment = fields.TextField(description="Some \n comment")
    commented_by = fields.TextField()

