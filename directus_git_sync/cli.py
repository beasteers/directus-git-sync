import logging


def main(key=None):
    logging.basicConfig()
    from . import apply, export, wipe, data, seed
    import fire
    fire.Fire({
        "apply": apply,
        "export": export,
        "wipe": wipe,
        "data": data,
        "seed": seed,
        # "api": API,
    })
