#ResDiary
Recommendations engine for ResDiary (https://www.resdiary.com/)

iTeam:
- Vladimir Bardarski (PeshoGoshov): 
- Paulius Dilkas (dilkas): 2146879d
- Dom Jurkus (domantasjurkus):
- Edward Kalfov (TheScouser):
- Josh O'brien (Josh-Dev): 
- Joseph O'Hagan (JosephOHagan): 2136120o 


#Generating recommendations
To run the recommendations script:
```
python src/main.py --alg=ALS --data=data/Booking.csv --out=/home/user/data/recommendations.csv
```
Alternatively if that fails...
```
spark-submit src/main.py --alg=ALS --data=data/Booking.csv --out=/home/user/data/recommendations.csv
```

`--alg`: [`als`, `implicit`]  
`--data`: relative (`src/data/Booking.csv`) or absolute (`/home/steve/Booking.csv`) path.  


#Presentation
`node server.js` will launch a server on `http://localhost:3000`.  
Recommendations are fetched by default from `src/data/Recommendations.csv`.  


##Testing
`python test.py` triggers tests and produces coverage reports in `coverage/`.


###Outside of lab access Jenkins / Trac / VM
Fire up the terminal and enter:
``` 
ssh -L 8000:130.209.251.67:80 -L 8080:130.209.251.67:8080 <yourGUID>@sibu.dcs.gla.ac.uk
```
- To access Jenkins go to http://localhost:8080/
- To access the Trac page visit http://localhost:8000/projects/myproject/
- To access the VM sign in as you do in the lab: ssh -i teamkey.pem yourUsername@130.209.251.67


###Dissertation Notes
- The dissertation must be prepared using the LaTeX template provided.
- The dissertation is a reflection on the team's experiences during the project.
- The dissertation should be a single PDF document of a maximum of 20 pages including front matter and references.
- The LaTeX source for the template, the dissertation and any assoicated figures must be present in your repository.
- REFERENCES: We are looking for academic references over others such as Wikipedia or personal hunches. 
- NOTE: The PDF itself should not be stored in the repository.  

Deadline:  Friday 24th March 2017 at 4pm
