# ResDiary
Recommendations engine for ResDiary (https://www.resdiary.com/)

# Something we need to test
Note on whatever just happened where it reset everything and I lost the README edit.

###Setting up GitHub repo on VM
We have an online GitHub repo for many reasons.

Its easier for the customer and probably easier to mark for the university staff. 

In reality we have two repos, the GitHub repo (setup after the customer asked if we'd set one of those up and we got the ok from Jeremy) and the repo that gets setup as part of the git setup tutorial sheet from week 3 (http://moodle2.gla.ac.uk/mod/page/view.php?id=555908)

While you can just make a local clone of a repo we should probably incorporate this into the VM in some way (marks). 

From what I can tell there are two ways we can go about this:
- Easiest method is just to connect to the VM then clone the GitHub repo - delete the teamproject repo that's also been setup.
- If we MUST use the teamproject repo you can clone the GitHub repo into that - this involves a commit twice process - repo within a repo - recursive repos ?

Recursive repos would be insane but also kind of fun but from a "proper software development methods" perspective I'd just suggest killing the teamproject repo and clone the GitHub repo into the VM.

###Outside of lab access to TRAC / VM
Fire up the terminal and enter:
``` 
ssh -L 8000:130.209.251.67:80 <yourGUID>@sibu.dcs.gla.ac.uk
```
- To access the TRAC page visit (http://localhost:8000/projects/myproject/)
- To access the VM just sign in as you do on the lab machines.
