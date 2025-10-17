from wtforms import widgets

class BS3SelectFieldWidget(widgets.Select):
    def __call__(self, field, **kwargs):
        kwargs["class"] = "form-control"
        if field.label:
            kwargs["placeholder"] = field.label.text
        if "name_" in kwargs:
            field.name = kwargs["name_"]
        return super(BS3SelectFieldWidget, self).__call__(field, **kwargs)
