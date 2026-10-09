"""Safe raw-image delivery formats; selection never implies conversion."""
LEGACY_IMAGE_EXTENSIONS = (".jpg", ".jpeg", ".png", ".webp", ".avif")
SUPPORTED_IMAGE_EXTENSIONS = LEGACY_IMAGE_EXTENSIONS + (".gif",)
DEFAULT_IMAGE_EXTENSIONS = SUPPORTED_IMAGE_EXTENSIONS


class ImageFormatError(ValueError):
    """A bounded format decision, never arbitrary publication text."""

    def __init__(self, image_format):
        known = (type(image_format) is str and len(image_format) <= 4
                 and "." + image_format in SUPPORTED_IMAGE_EXTENSIONS)
        self.image_format = image_format if known else "unsupported"
        self.code = "IMAGE_FORMAT_DISABLED" if known else "IMAGE_FORMAT_UNSUPPORTED"
        super().__init__(self.code)


def image_extensions(value=None):
    if value is None:
        return DEFAULT_IMAGE_EXTENSIONS
    if isinstance(value, str):
        value = value.split(",")
    if not isinstance(value, (list, tuple)) or not value:
        raise ValueError("nonempty image extensions required")
    if any(type(x) is not str or x not in SUPPORTED_IMAGE_EXTENSIONS for x in value):
        raise ValueError("unsupported image extension")
    if len(set(value)) != len(value):
        raise ValueError("duplicate image extension")
    return tuple(x for x in SUPPORTED_IMAGE_EXTENSIONS if x in value)


def image_filename(image_format, extensions=None):
    enabled = image_extensions(extensions)
    if (type(image_format) is not str or len(image_format) > 4
            or "." + image_format not in enabled):
        raise ImageFormatError(image_format)
    return "image." + image_format
