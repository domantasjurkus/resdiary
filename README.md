# ResDiary
Recommendations engine for ResDiary (https://www.resdiary.com/)

iTeam:
- Vladimir Bardarski (PeshoGoshov): 
- Paulius Dilkas (dilkas):
- Dom Jurkus (domantasjurkus):
- Edward Kalfov (TheScouser):
- Josh O'brien (Josh-Dev): 
- Joseph O'Hagan (JosephOHagan): 2136120o 

###Outside of lab access to VM / Trac / Jenkins
Fire up the terminal and enter:
``` 
ssh -L 8000:130.209.251.67:80 -L 8080:130.209.251.67:8080 <yourGUID>@sibu.dcs.gla.ac.uk
```
- To access the Trac page visit http://localhost:8000/projects/myproject/
- To access the VM sign in as you do in the lab: ssh -i teamkey.pem yourUsername@130.209.251.67
- To access Jenkins go to http://localhost:8080/

###To make the VM terminal normal - maybe don't do this as it might be the cause of my profile corruption (Joseph)
As I'm sure your aware the VM default terminal is as basic as it gets.
Dom tried to fix his but everything caught fire and he bailed.
To get the nice one with the colours and the auto-complete after signing in using your username enter the following:
```
sudo usermod -s /bin/bash <yourUsername>
```
