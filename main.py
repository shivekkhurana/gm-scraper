import datetime
import grequests
import csv
import json
import copy

input_profile = {
  "boy_dob":"1994-11-21",
  "boy_lat":28.613939,
  "boy_lng":77.209021,
  "boy_name":"Shivek Khurana",
  "boy_timezone":"Asia/Calcutta",
  "boy_tob":"16:23",
}

num_requests = 100

def canditate_gen(boy):
	candidates = []

	boy_date = datetime.datetime.strptime(boy['boy_dob'],'%Y-%m-%d')
	girl_startdate = boy_date + datetime.timedelta(days=-5*365)
	girl_stopdate = boy_date + datetime.timedelta(days=5*365)

	while(girl_startdate<=girl_stopdate):
		couple = copy.deepcopy(boy)
		couple['girl_lat'] = boy['boy_lat']
		couple['girl_lng'] = boy['boy_lng']
		couple['girl_timezone'] = boy['boy_timezone']
		couple['girl_name'] = boy['boy_name']+'\'s girlfiend'
		girl_startdate = girl_startdate + datetime.timedelta(minutes=20)
		
		couple['girl_dob'] = datetime.datetime.strftime(girl_startdate,'%Y-%m-%d')
		couple['girl_tob'] = datetime.datetime.strftime(girl_startdate,'%H:%M')

		candidates.append(couple)

	return candidates


def make_row(response):
	gunas = response['Gunas']
	return "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}".format(response['girl_dob'] + ' ' + response['girl_tob'],response['boy_dob'], response['girl_dob'], response['boy_tob'], response['girl_tob'], gunas['Varna']['boy'], gunas['Varna']['girl'], gunas['Varna']['score'],gunas['Vasya']['boy'], gunas['Vasya']['girl'], gunas['Vasya']['score'],gunas['Tara']['boy'], gunas['Tara']['girl'], gunas['Tara']['score'],gunas['Yoni']['boy'], gunas['Yoni']['girl'], gunas['Yoni']['score'],gunas['Graha Maitri']['boy'], gunas['Graha Maitri']['girl'], gunas['Graha Maitri']['score'],gunas['Gana']['boy'], gunas['Gana']['girl'], gunas['Gana']['score'],gunas['Bhakoot']['boy'], gunas['Bhakoot']['girl'], gunas['Bhakoot']['score'],gunas['Nadi']['boy'], gunas['Nadi']['girl'], gunas['Nadi']['score'],response['Score'])


def get_initial_i():
	row_count = sum(1 for row in csv.reader( open('results.csv') ) ) 
	return row_count

all_candidates = canditate_gen(input_profile)

f = csv.writer(open("results.csv", "a"))

def main():

	try:
		i = get_initial_i()
		while i < len(all_candidates):
			#async processing of {num_requests} request at once
			print("Processing batch " + str(i) + " to " + str(i+num_requests))
			rs = [grequests.post("http://gun-milap-backend.dev/match", data=all_candidates[j]) for j in range(i,i+num_requests)]
			i+=num_requests
			results = grequests.map(rs) ## List of json objects (the results)
			results = [r.json() for r in results]
			for x in results:
				f.writerow(make_row(x).split(','))

	except KeyboardInterrupt:
		print 'Exit'

	except:
		print('Error ! Restarting\n------------------------------')
		main()


if __name__ == '__main__':
	main()
