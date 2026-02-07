from django import template
import os

register = template.Library()


#### Add this to settings.py in TEMPLATES OPTIONS
# "libraries": {
#     "my_tags": "templatetags.my_tags",
# },
###
@register.simple_tag
def get_env_var(key):
    return os.environ.get(key, "")
