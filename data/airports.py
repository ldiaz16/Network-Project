import urllib.request

# Download the airlines data from GitHub
urllib.request.urlretrieve(
    "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat",
    "airports.dat"
)