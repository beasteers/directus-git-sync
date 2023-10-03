import logging



def main(key=None):
    logging.basicConfig()
    from . import apply, export, wipe
    from starstar.argparse import Star
    d = {
        "apply": apply,
        "export": export,
        "wipe": wipe,
        # "api": API,
    }
    Star(d[key] if key else d, description="""
Managing Directus as a git repository!
    """, env_format="DIRECTUS")

def apply():
    main("apply")

def export():
    main("export")
