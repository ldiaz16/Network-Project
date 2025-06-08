import urllib.request

# Download the routes data from GitHub
urllib.request.urlretrieve(
    "https://raw.githubusercontent.com/jpatokal/openflights/master/data/routes.dat",
    "routes.dat"
)