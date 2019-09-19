Github Setup Commands

### BTD in github
```
    https://github.com/bobbydreamer/Udacity-Data_Wrangling.git
```

### Setup commands 
```
git init 
git status 
git add .
git status 
git commit -m 'Uploading Udacity Data Wrangling Project 1'
git status 
git remote add origin https://github.com/bobbydreamer/Udacity-Data_Wrangling.git

Required forced updated, so added -f below if you had created the repository from github.com and added a readme.md as when uploading it would error out with below message.
git push -u -f origin master
```
### Details on error
```
PS D:\BigData\12. Python\Udacity\Project 1 - Data Wrangling\Udacity - Sushanth bobby - Project 1 - Wrangle and Analyze Data> git push -u origin master
To https://github.com/bobbydreamer/Udacity-Data_Wrangling.git
 ! [rejected]        master -> master (non-fast-forward)
error: failed to push some refs to 'https://github.com/bobbydreamer/Udacity-Data_Wrangling.git'
hint: Updates were rejected because the tip of your current branch is behind
hint: its remote counterpart. Integrate the remote changes (e.g.
hint: 'git pull ...') before pushing again.
hint: See the 'Note about fast-forwards' in 'git push --help' for details
```

### Everyday commands
```
git status
git add .
git status
git commit -m 'Added filename'
git push 

Amending last commit
git commit --amend --no-edit

```
