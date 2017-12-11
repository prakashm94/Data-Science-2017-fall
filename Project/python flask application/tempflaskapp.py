from flask import Flask, render_template, request
import requests
import urllib
import urllib.request
import json

app = Flask(__name__)

@app.route('/result', methods=['POST'])
def result():
	year = request.form['year']
	day = request.form['day']
	month = request.form['month']
	week = request.form['week']
	deploc = request.form['origin']
	arrloc = request.form['dest']
	arrtime = request.form['arrtime']
	deptime = request.form['deptime']
	crsarrtime = request.form['crsarrtime']
	crsdeptime = request.form['deptime']
	crselaptime = request.form['cet']
	visib = request.form['visbl']
	wind = request.form['winds']
	presipe = request.form['pre']



	data = {
	        "Inputs": {
	                "input1":
	                [
	                    {
	                            'Year': year,   
	                            'Day': day,   
	                            'Month': month,   
	                            'Week': week,   
	                            'Origin': deploc,   
	                            'Dest': arrloc,   
	                            'ArrTime': arrtime,   
	                            'DepTime': deptime,
	                            'CRSArrTime': crsarrtime,   
	                            'CRSDepTime': crsdeptime,   
	                            'CRSElapsedTime': crselaptime,   
	                            'ActualElapsedTime': "155",   
	                            'ArrDelayMinutes': "0",    
	                            'Flight_Status': "0",   
	                            'OriginVisibility': visib,   
	                            'OriginWind': wind,   
	                            'OriginPrecip': presipe,   
	                    }
	                ],
	        },
	    "GlobalParameters":  {
	    }
	}

	body = str.encode(json.dumps(data))

	url = 'https://ussouthcentral.services.azureml.net/workspaces/c2f8dc5126ac4024b08690eda8426a56/services/7df1a9f8d4774c7ea716d1a501c1fc72/execute?api-version=2.0&format=swagger'
	api_key = 'erIb/2RD+qzfb4wXNsqA8KQFfaRj8xxmOVELuR+AcPCSNAhVT/0A9dy7iJHtoih7HI4B/LQQ+U2abHEjBnKThQ==' 
	headers = {'Content-Type':'application/json', 'Authorization':('Bearer '+ api_key)}
	req = urllib.request.Request(url, body, headers)

	try:
	    response = urllib.request.urlopen(req)

	    result = response.read()
	except urllib.error.HTTPError as error:
		result = "error"
	result = json.loads(result.decode("utf-8"))
	status = result['Results']['output1'][0]['ArrDelayMinutes']

	#json_object = response.json
	#r = json_object['output1']['ArrDelayMinutes']
	return render_template('home.html',delaytime = status)

@app.route('/')
def index():
    return render_template('home.html')

if __name__ == '__main__':
	app.run(debug=True)