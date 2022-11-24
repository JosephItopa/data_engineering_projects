impoert numpy as np
import pandas as pd

df_advent, df_crime, df_horror, df_mystery, df_sci, df_thriller = pd.read_csv('adventure.csv'),pd.read_csv('crime.csv'),pd.read_csv('horror.csv'),\
                                                                    pd.read_csv('mystery.csv'), pd.read_csv('sci-fi.csv'), pd.read_csv('thriller.csv')

df_advent['genre'], df_crime['genre'], df_horror['genre'],df_mystery['genre'], df_sci['genre'], df_thriller['genre'] = \
                                                                                    'adventure', 'crime', 'horror', 'mystery', 'scifi', 'thriller'

df = pd.concat([df_advent, df_crime, df_horror, df_mystery, df_sci, df_thriller])

df['year'] = df['year'].str.replace('(', '', regex=True)
df['year'] = df['year'].str.replace(')', '', regex=True)
df['year'] = df['year'].str.replace('I 2015', '2015', regex=True)
df['year'] = df['year'].str.replace('I 2017', '2017', regex=True)
df['votes'] = df['votes'].str.replace(',', '', regex=True)
df['metascore'] = df['metascore'].str.replace(' \n', '', regex=True)
df['metascore'] = df['metascore'].str.replace(' Metascore', '', regex=True)
df['time'] = df['time'].str.replace(' min', '', regex=True)

df['num'], df['title'], df['ye'] = df['title'].str.split('\n', 2).str
df['vot'], df['votes'], df['grss'], df['gross'] = df['votes'].str.split('\n', 3).str

#df = df.drop(['num', 'ye', 'grss'], axis=1)
df = df[['title', 'year', 'certificate', 'time', 'genre', 'rating', 'metascore', 'simple_desc', 'votes', 'gross']]
df['gross'] = df['gross'].str.replace('$', '', regex=True)
df['gross'] = df['gross'].str.replace('M', '', regex=True)
df['gross'] = df['gross'].fillna(0)

df.rename(columns = {'time':'time(min)', 'simple_desc':'description', 'gross':'gross_earning(M)'}, inplace = True)

df['title'] = df['title'].astype(str)
df['year'] = df['year'].astype(int)
df['certificate'] = df['certificate'].astype(str)
df['time(min)'] = df['time(min)'].astype(int)
df['genre'] = df['genre'].astype(str)
df['rating'] = df['rating'].astype(float)
df['metascore'] = df['metascore'].astype(int)
df['description'] = df['description'].astype(str)
df['votes'] = df['votes'].astype(int)
df['gross_earning(M)'] = df['gross_earning(M)'].astype(float)

df.to_csv('clean-movie-review.csv', index=False)