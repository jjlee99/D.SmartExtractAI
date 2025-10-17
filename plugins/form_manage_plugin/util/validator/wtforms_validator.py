from wtforms.validators import ValidationError
import json

class JsonValidator(object):
    def __init__(self, message=None):
        if not message:
            message = "유효한 JSON 문자열이 아닙니다."
        self.message = message

    def __call__(self, form, field):
        try:
            json.loads(field.data)
        except (ValueError, TypeError):
            raise ValidationError(self.message)
