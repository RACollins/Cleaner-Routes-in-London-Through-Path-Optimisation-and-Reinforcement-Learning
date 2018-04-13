import pulp
import pandas as pd
import psycopg2

con = psycopg2.connect("dbname=project")
query = """
    SELECT eid, gamma, distance, startnode, endnode, geom
    FROM model2.edges;
"""

# create data frame and define variables
df = pd.read_sql(query, con)
pollution = df['gamma'] * df['distance']
distance = df['distance']
D = 5000

# define lp model with variables and constraints
model = pulp.LpProblem("Discrete Pollution Minimisation Problem", pulp.LpMinimize)
x_name = pulp.LpVariable.dicts("x", (i for i in df.index), lowBound=0, upBound=1, cat="Integer")
min = pulp.LpAffineExpression([(x_name[i], pollution[i]) for i in df.index])
model += pulp.LpConstraint(
    pulp.LpAffineExpression(
        [(x_name[i], distance[i]) for i in df.index]
    ), sense=pulp.constants.LpConstraintEQ, name="threshold", rhs=D
)

model.setObjective(min)

model.solve()

print("Model status: {0}".format(pulp.LpStatus[model.status]))

# convert variables to row indicies if their value is 1
found_indicies = [int(v.name[2:]) for v in model.variables() if v.varValue == 1]

# conditionally select edge IDs
idSeq = df.iloc[found_indicies]["eid"].astype(float)

# set inpath to false for all edges
update_query = "UPDATE model2.edges SET inpath=false;"
cur = con.cursor()
cur.execute(update_query)

# set inpath to true for all edges in optimal path
for eid in idSeq:
    tuple = [eid]
    cur.execute("UPDATE model2.edges SET inpath=true WHERE eid=%s;", tuple)

con.commit()


# check whether D == sum of distances
print("Sum of path distances: {0}m".format(int(df.iloc[found_indicies]["distance"].sum())))

print("Done!")
