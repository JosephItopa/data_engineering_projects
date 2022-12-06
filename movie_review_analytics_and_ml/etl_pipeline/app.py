import pandas as pd
import numpy as np
from extract import get_movies

url = "https://www.imdb.com/search/title/?genres=Adventure&sort=user_rating,desc&title_type=feature&num_votes=25000,&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=5aab685f-35eb-40f3-95f7-c53f09d542c3&pf_rd_r=N97GEQS6R7J9EV7V770D&pf_rd_s=right-6&pf_rd_t=15506&pf_rd_i=top&ref_=chttp_gnr_16"
genres = [
    "adventure",
    "crime",
    "horror",
    "mystery",
    "sci-fi",
    "thriller"
]
url_dict = {}
for genre in genres:
    formated_url = url.format(genre)
    url_dict[genre] = formated_url
    
for genre, url in url_dict.items():
    get_movies(url, 1, genre+'.csv')
    print("Saved:", genre+'.csv')