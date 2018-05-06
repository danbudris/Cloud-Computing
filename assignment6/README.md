# Tracking Congressional Tweets

## Author
- Dan Budris
- DBudris@bu.edu
- 4/30/2018
- CS 755, Cloud Computing

## Overview
One of the most important impacts of modern social media is how we conduct ourselves in the political sphere.
There has been much attention to how social media impacts our perceptions, motivates/manipulates voters, and allows our 
elected officals to directly express themselves (for better or for worse).

## What I've Done
To that end, I've modified the Tweepy code to track reply tweets involving the U.S. Senate. I've downloaded information about members of 
the Senate to a JSON file, extracted their twitter handles, and loaded them into the script.  The script then tracks only tweets that are a reply and involved @ a member of the U.S. Senate.  We then track the hashtags in those tweets, who they are directed at, and who they are in reply to. 

Even by simply looking at the printed output we can actively see who is involved in controversial issues right now -- just look up the top
@ names, and scan the replies they are involved in, and glance at the hashtags trending, and you'll immediately see conflict, controversy, and conversations that are domininant across multiple media platforms.  

## Where this could go
We can begin to imagine more complex data science projects that mine this information for trends in current events, voting patterns, etc.
For example, it'd be extremely interesting to track these on a graph over the course of a day, or in real time, and be able to view
what is trending and how a converation involving a given candidate has evolved as other weigh in.  Correlating this with traditonal media coverage and the replies of media organizations (of which you'll see plenty in the replies and hashtags) would provide an interesting window into how media interacts with the social media. 

## The Data
The data  is from the ProPublica Congress API (https://projects.propublica.org/api-docs/congress-api/), and open project to track all of U.S. Congress in detail.  

## Note
The scripts have been enriched with argument parsing and main functions to promote re-use and portability.
