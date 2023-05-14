# Build flow etl medical data

## Source data:
Medical data of ThaiBinh province, stored in MongoDb  
Preview schema of database:  
<center> <img src="img/dataMongo.png" alt= “” width="600" height="value"> </center> <br> <br>
<center> <img src="img/mongo.png" alt= “” width="600" height="value"> </center>
<br>  

## Data modeling:
Some Fact table and Dimension table:  
<center> <img src="img/dataModeling.png" alt= “” width="600" height="value"> </center>

## Data flow:
After consider runtime of all tasks, pipeline is constructed as follows:
<center> <img src="img/flow.png" alt= “” width="700" height="value"> </center>

## Result of Etl process:
<center> <img src="img/clh1.png" alt= “” width="600" height="value"> </center>
<br><br>
<center> <img src="img/clh2.png" alt= “” width="600" height="value"> </center>