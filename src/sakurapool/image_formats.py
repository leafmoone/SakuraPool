"""Safe raw-image delivery formats; selection never implies conversion."""
DEFAULT_IMAGE_EXTENSIONS = (".jpg", ".jpeg", ".png", ".webp", ".avif")
SUPPORTED_IMAGE_EXTENSIONS = DEFAULT_IMAGE_EXTENSIONS + (".gif",)


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
    if type(image_format) is not str or "." + image_format not in image_extensions(extensions):
        raise ValueError("image format not enabled")
    return "image." + image_format
