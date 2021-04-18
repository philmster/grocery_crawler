# ----------------------------------------------------------------------------------------------------------------------
from pywebcopy import save_website

# ----------------------------------------------------------------------------------------------------------------------
url = "https://www.edeka24.de/"
dirDownload = "C:\\Users\\Beyond\\Downloads"

kwargs = {"bypass_robots": True, "project_name": "recognisable-name", "load_css": False, "load_images": False,
          "load_javascript": False}

save_website(url=url, project_folder=dirDownload, **kwargs)

# ----------------------------------------------------------------------------------------------------------------------
