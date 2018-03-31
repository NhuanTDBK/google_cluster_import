import re

def downcase(name):
    text = name[0].upper() + name[1:]
    text_concat = '_'.join(re.findall('[A-Z][^A-Z]*',text))
    return text_concat[0].lower() + text_concat[1:].lower()

def convert_downcase(obj=None):
    keys = obj.keys()
    obj_new = {}

    for key in keys:
        val = obj[key]
        if key == "_id":
            obj_new[key] = val
        else:
            obj_new[downcase(key)] = val

    return obj_new
