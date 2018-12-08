import csv
import os
import os.path
import whoosh
import string
import json
from tempfile import NamedTemporaryFile
import shutil
from flask import Flask, render_template, url_for, request
from whoosh.index import create_in
from whoosh.index import open_dir
from whoosh.fields import Schema
from whoosh.fields import TEXT, ID
from whoosh.fields import NUMERIC
from whoosh.qparser import QueryParser
from whoosh.qparser import MultifieldParser
from whoosh.query import Variations
from whoosh import qparser


app = Flask(__name__)


@app.route('/', methods=['GET', 'POST'])
def index():
    return render_template('welcome_page.html')


@app.route('/my-link/')
def my_link():
    return 'Click'


@app.route('/results/', methods=['GET', 'POST'])
def results():
    global mysearch
    if request.method == 'POST':
        data = request.form
    else:
        data = request.args

    keywordquery = data.get('searchterm')
    pageNum = int(data.get('pageNum'))

    id, Name, Description, Genre, Yearofrelease, Rating, ImdbUrl, Votes = mysearch.search(
        keywordquery, pageNum)

    return render_template('results.html', query=keywordquery, results=list(zip(id, Name, Description, Genre, Yearofrelease, Rating, ImdbUrl, Votes)))


class MyWhooshSearch(object):
    """docstring for MyWhooshSearch"""
    def __init__(self):
        super(MyWhooshSearch, self).__init__()

    def updateDocument(self, url, votes, rating, name, description, genre, date):
        writer = self.indexer.writer()
        writer.update_document(ImdbUrl=url, Rating=rating, Votes=votes, Name = name, Description = description, Genre = genre, Yearofrelease = date)
        writer.commit()
        
        tempfile = NamedTemporaryFile(mode='w', delete=False)
        fields = ['Name', 'Desc', 'Date', 'Rating', 'Genre', 'Url', 'Votes']

        with open('original.csv', 'r') as csvfile, tempfile:
            reader = csv.DictReader(csvfile, fieldnames=fields)
            writer = csv.DictWriter(tempfile, fieldnames=fields)
            for row in reader:
                if row['Url'] == str(url):
                    row['Rating'], row['Votes'] = rating, votes
                row = {'Name': row['Name'], 'Desc': row['Desc'], 'Date': row['Date'], 'Rating': row['Rating'], 'Genre': row['Genre'], 'Url': row['Url'], 'Votes': row['Votes']}
                writer.writerow(row)

        shutil.move(tempfile.name, 'original.csv')

    def search(self, queryEntered, pageNum):
        id = list()
        Name = list()
        Genre = list()
        Yearofrelease = list()
        Description = list()
        Rating = list()
        ImdbUrl = list()
        Votes = list()

        with self.indexer.searcher() as search:
            # fileds to be parsed are Name, Description, Genre, and Year of Release
            query = MultifieldParser(['Name', 'Description', 'Genre', 'Yearofrelease', 'Rating',
                                      'ImdbUrl', 'Votes'], schema=self.indexer.schema, termclass=Variations)
            query = query.parse(queryEntered)
            results = search.search_page(query, pagenum=pageNum)
            i = 0
            for x in results:
                id.append(i)
                Name.append(x['Name'])
                Description.append(x['Description'])
                Genre.append(x['Genre'])
                Yearofrelease.append(x['Yearofrelease'])
                Rating.append(x['Rating'])
                ImdbUrl.append(x['ImdbUrl'])
                Votes.append(x['Votes'])
                i = i + 1

        return id, Name, Description, Genre, Yearofrelease, Rating, ImdbUrl, Votes

    def index(self):
        schema = Schema(
            Name=TEXT(stored=True),
            Description=TEXT(stored=True),
            Yearofrelease=TEXT(stored=True),
            Rating=TEXT(stored=True),
            Genre=TEXT(stored=True),
            ImdbUrl=ID(stored=True, unique=True),
            Votes=TEXT(stored=True)
        )

        if not os.path.exists("indexdir"):
            os.mkdir("indexdir")

        indexer = create_in("indexdir", schema)
        writer = indexer.writer()

        csvfile = open('original.csv', 'rt', encoding='utf-8')
        rows = csv.reader(csvfile, delimiter=',')

        for row in rows:
            cell_count = 1
            for cell in row:
                if cell_count == 1:
                    Name = cell
                elif cell_count == 2:
                    Description = cell
                elif cell_count == 3:
                    Yearofrelease = cell
                elif cell_count == 4:
                    Rating = cell
                elif cell_count == 5:
                    Genre = cell
                elif cell_count == 6:
                    ImdbUrl = cell
                elif cell_count == 7:
                    Votes = cell
                elif cell_count == 8:
                    img = cell
                cell_count += 1
            writer.add_document(Name=Name, Description=Description, Yearofrelease=Yearofrelease,
                                Rating=Rating, Genre=Genre, ImdbUrl=ImdbUrl, Votes=Votes)
        writer.commit()
        self.indexer = indexer
	
@app.route('/update/', methods=['GET', 'POST'])
def update():
	global mysearch
	if request.method == 'POST':
		data = request.form
	else:
		data = request.args

	name = data.get('name')
	description = data.get('desc')
	genre = data.get('genre')
	date = data.get('date')    
	url = data.get('url')
	rating = data.get('rating')
	votes = data.get('votes')

	mysearch.updateDocument(url, votes, rating, name, description, genre, date)

	return json.dumps({'status': 'OK', 'url': url, 'rating': rating, 'votes': votes})


if __name__ == '__main__':
    global mysearch
    mysearch = MyWhooshSearch()
    mysearch.index()
    app.run(debug=True)
