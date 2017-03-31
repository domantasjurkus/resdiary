# ResDiary
Recommendations engine for ResDiary (https://www.resdiary.com/)

## iTeam:
- Vladimir Bardarski (PeshoGoshov): 
- Paulius Dilkas (dilkas): 2146879d
- Dom Jurkus (domantasjurkus):
- Eduard Kalfov (TheScouser): 2129029k
- Josh O'brien (Josh-Dev): 
- Joseph O'Hagan (JosephOHagan): 2136120o 

# IMPORTANT!
The recommendations generated from the provided test files would probably be inaccurate, since the dataset is small.
If you want accurate results the dataset should be at least 100K.

# Installation
1. Run deployment.sh: `./deployment.sh` which should automatically install all required components.
2. Before running Spark make sure you can run it globally with the following command: <br>
``` PATH=$PATH:~/spark-2.1.0-bin-hadoop2.7/bin/ ```
<br> Otherwise, you'll have to explicitly specify spark-submit which is quite long: <br>
`~/spark-2.1.0-bin-hadoop2.7/bin/spark-submit`

**If you don't want to do this every time then consider adding this command to your bash profile or /etc/environment.**

**If the deployment script fails then:** 

1. Go to http://spark.apache.org/downloads.html and download version 2.1.0 of Apache Spark.
2. Unzip the archive in your home folder for example
3. Run `pip install requirements.txt`
4. Follow step 2 which shows how to make Spark global for the current terminal instance.

# Generating recommendations
To train and predict a model based on a given dataset:
```
spark-submit src/main.py --alg=ExplicitALS --data=tests/stubs/datastubs/stub_bookings.txt --out=recommendations.csv --func=execute
```

- `--alg`: [`explicitals`, `implicitals`, `contentbased`, `pricepoint`, `system`]
- `--data`(input) `--out`(output): relative (`src/data/Bookings.csv`) or absolute (`/home/steve/Bookings.csv`) paths
- `--func`: [`execute`, `evaluate`, `train`]
- `--load`(optional): [`true`]

A few examples that are specific for ALS and System recommenders:

1. To find the best possible hyper parameters:<br>
``` spark-submit src/main.py --alg=ExplicitALS --data=tests/stubs/datastubs/stub_bookings.txt --func=train ```

2. To train the algorithm and then generate recommendations:<br>
``` spark-submit src/main.py --alg=System --data=tests/stubs/datastubs/stub_bookings.txt --out=data/recommendations.csv --func=execute ```

3. To load an existing model and then generate recommendations:<br>
``` spark-submit src/main.py --alg=System --data=tests/stubs/datastubs/stub_bookings.txt --out=data/recommendations.csv --load=true --func=execute ```

4. To evaluate a model:<br>
``` spark-submit src/main.py --alg=ExplicitALS --data=tests/stubs/datastubs/stub_bookings.txt --func=evaluate ```

# Detailed guide

### System recommender
- The System recommender combines other recommenders by giving weights to them. They can be configured in default.cfg.
- Running System recommender in `--func=train` mode will result in writing the best possible weights in default.cfg.

### ExplicitALS
- This recommender should be used on a large dataset which includes ratings given explicitly by users.
- On 1M rows the Mean Squared Error is around 0.4 for the default parameters in the configuration file which suggests that the algorithms is quite accurate.

### ImplicitALS
- This recommender should be used on a large dataset with ratings inferred from data sources such as browser history,visiting frequency and etc. 

### CuisineType
- This content based recommender finds out what cusine types users like. Then it recommenders restaurants with similar cuisine types. This recommender should almost always have weight of 1.

### PricePoint
- This content based recommender is not dependent on the size of the dataset. It should be given higher weight when the dataset consists of users who haven't rated restaurants at all.

### Database connection
- Since we don't have the complete schema of ResDiary tables, we didn't write query/write functions for a database. They should be very easy to implement, since Spark allows the queried data to be saved in DataFrames which we use. 

## Config options
### Default params
- training_percentage: Specifies what percentage of the data will be used for training. 80% is usually the best value
- data_dir: Specifies the folder where the restaurants and cuisine type data should be placed.
- lat_diff: Latitude difference. Location filtering works by comparing the latitude and longtitude differences of restaurants.
- long_diff: Longtitude difference. Increase if you want to recommende restaurants in a bigger range.
- schema: The header of the output files
- restaurant_file: Specifies the name of the file which contains restaurants data.
- Other default parameters: Specify min,max values and the incrementing step for hyper parameters. These are used to train better models.

### System
- recs_per_user: How many recommendations per user to generate
- maximum_weight: Weights are generate in the inverval [0, maximum_weight]

### Recommender specific:
- ALS recommenders include best hyper parameters
- CuisineType specifies minimum_like_score = 4 as default. This means that only cuisine types rated with 4 and 5 would be considered when recommending restaurants. It also uses specific file that contains cuisine types data.

### Universal
- weight: How important is the score of this recommender. This is considered only in System recommender. For example, if ExplicitALS has weight 2, CuisineType has weight 1 and PricePoint has weight 1 then the score of the System recommender is mostly based on the ExplicitALS.

#  Presentation Mode
`npm install` to install all required components.
`node server.js` will launch a server on `http://localhost:3000`.    

##  Testing
`spark-submit test.py` triggers tests and produces coverage reports in `coverage/`.


###  Outside of lab access Jenkins / Trac / VM
Fire up the terminal and enter:
``` 
ssh -L 8000:130.209.251.67:80 -L 8080:130.209.251.67:8080 <yourGUID>@sibu.dcs.gla.ac.uk
```
- To access Jenkins go to http://localhost:8080/
- To access the Trac page visit http://localhost:8000/projects/myproject/
- To access the VM sign in as you do in the lab: ssh -i teamkey.pem yourUsername@130.209.251.67


###  Dissertation Notes
- The dissertation must be prepared using the LaTeX template provided.
- The dissertation is a reflection on the team's experiences during the project.
- The dissertation should be a single PDF document of a maximum of 20 pages including front matter and references.
- The LaTeX source for the template, the dissertation and any assoicated figures must be present in your repository.
- REFERENCES: We are looking for academic references over others such as Wikipedia or personal hunches. 
- NOTE: The PDF itself should not be stored in the repository.  

Deadline:  Friday 31st March 2017


### Configuration
- Training ranges for ALS recommenders. Adjust them if the optimal value is very close to the min or max range value. Max should always be strictly greater than min because the grid search interval is [min, max).
- The minimum review score from a booking for a cuisine type to be considered liked.
- Default price point to use if there is not enough data to calculate averages (only used in exceptional circumstances with very little data).
