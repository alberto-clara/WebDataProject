import csv
import os, os.path
import whoosh
import string
import json
from flask import Flask, render_template, url_for, request
from whoosh.index import create_in
from whoosh.index import open_dir
from whoosh.fields import Schema
from whoosh.fields import TEXT
from whoosh.fields import NUMERIC
from whoosh.qparser import QueryParser
from whoosh.qparser import MultifieldParser
from whoosh.query import Variations
from whoosh import qparser


app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def index():
	print('HEya')
	return render_template('welcome_page.html')

@app.route('/my-link/')
def my_link():
	print('clicked')
	return 'Click'

@app.route('/results/', methods=['GET', 'POST'])
def results():
	global mysearch
	if request.method == 'POST':
		data = request.form
	else:
		data = request.args

	keywordquery = data.get('searchterm')
	#test = data.get('test')
	pageNum = int(data.get('pageNum'))
	print('Keyword Query is: ' + keywordquery)
	#print('Test Query is: ' + test)

	id, Name, Description, Genre, Yearofrelease, Rating, ImdbUrl, Votes = mysearch.search(keywordquery, pageNum)
	
	return render_template('results.html', query=keywordquery, results=list(zip(id, Name, Description, Genre, Yearofrelease, Rating, ImdbUrl, Votes)))


class MyWhooshSearch(object):
	"""docstring for MyWhooshSearch"""
	def __init__(self):
		super(MyWhooshSearch, self).__init__()

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
			query = MultifieldParser(['Name', 'Description', 'Genre', 'Yearofrelease', 'Rating', 'ImdbUrl', 'Votes'], schema=self.indexer.schema, termclass=Variations)
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
		ImdbUrl=TEXT(stored=True),
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
			writer.add_document(Name = Name, Description = Description, Yearofrelease = Yearofrelease, Rating = Rating, Genre = Genre, ImdbUrl = ImdbUrl, Votes = Votes)
			
		writer.commit()
		self.indexer = indexer


@app.route('/update/', methods=['GET', 'POST'])
def update():
	global mysearch
	if request.method == 'POST':
		data = request.form
	else:
		data = request.args

	url = data.get('url')
	return json.dumps({'status':'OK', 'url' : url});

if __name__ == '__main__':
	global mysearch
	mysearch = MyWhooshSearch()
	mysearch.index()
	app.run(debug=True)
