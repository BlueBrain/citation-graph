
## Creating Perspectives in Neo4J and Bloom

These are the perspectives we intend to show for demonstration purposes.

Before making any perspectives, make sure that 

```bash
dbms.security.auth_enabled=true
```

is set for the DB.

In the ```Graph Apps``` go to ```Bloom```.
By default an empty perspective will be shown if none are created yet.
On the top left corner click ```Perspective``` and in the popup window click on the name of the 
perspective to get to the perspectives Dashboard.
You can use the ```Create``` button to create an empty perspective.
For each perspective you should create a saved cypher query that will load the nodes and edges.
For all perspectives, always check the ```Hide unrecognized nodes``` option.
When stepping into a Perspective, always ```Clear scene``` before loading any nodes and edges.
