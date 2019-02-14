##################
# @ Hezha Hassan
# Python code to import two columns data into Neo4j database.,
# Usage: python neo4j_solution.py path/the/file.tsv
#
# You are welcome to contact me under : hezha.hassan@hezhalab.com
#################

import sys
from py2neo import Graph, Node, Relationship


class neo4j_solution():
    graph = None
    def __init__(self,u,p):
        self.graph = Graph(user=u, password=p)

    def neo4j_connection(self):
        try:
            self.graph.run("MATCH (n) RETURN n").data()
            print ("Connection established successfully...")
        except:
            print ("Error: Your username or password is wrong ")
            sys.exit(1)

    def is_node_exist(self,x):
        z = self.graph.nodes.match("Person", name=str(x)).first()
        if z == None:
            return False
        return True

    def importing_to_database(self,j,m):
        N1 = Node("Person", name=str(j))
        N2 = Node("Person", name=str(m))
        if not self.is_node_exist(j):
            self.graph.create(N1)
        elif not self.is_node_exist(m):
            self.graph.create(N2)
        rel = Relationship(N1,"ISCONNECTED",N2)
        tx = self.graph.begin()
        # tx.create(N1) # to create a node
        tx.merge(N1,"Person","name")
        tx.merge(N2,"Person","name")
        tx.merge(rel)
        tx.commit()

    def read_file(self):
        print ("\nReading the file and importing into the database...")
        try:
            f = open(sys.argv[1], 'r')
        except:
            print ("\n---------------")
            print ("Error: File not found :( \nPlease make sure you run the"
                   +" script in this format:")
            print ("python neo4j_solution.py path/to/file.tsv")
            print ("---------------")
            sys.exit(1)
        for i in f:
            n1 = i.strip().split()[0]
            n2 = i.strip().split()[1]
            self.importing_to_database(n1,n2)
        print ("Data were imported successfully :) ")

    def MaxNode_outInDegree(self):
        print("\nFinding Node(s) with highest outgoing number...")
        n = None
        OD = 0
        Dic = {}
        q = self.graph.run\
        ("MATCH(n:Person)-[:ISCONNECTED]->(p:Person) RETURN n, count(*) AS Outdegree ORDER BY Outdegree DESC LIMIT 50")\
        .data()
        for i in q:
            if n == None:
                n = str(i["n"]).split("\'")[1]
                OD = i["Outdegree"]
                Dic[n] = OD
            elif OD == i["Outdegree"]:
                n = str(i["n"]).split("\'")[1]
                OD = i["Outdegree"]
                Dic[n] = OD
            else:
                break
        for i,j in Dic.items():
            n = str(i).strip()
            query = "MATCH(n:Person {name:\"%s\"})<-[:ISCONNECTED]-(p:Person) RETURN p" %str(n)
            q = self.graph.run(query).data()
            print ("The Node: ",i," has ",j," Outgoing and ",str(len(q))," Ingoing relations")

    def MinNode_InDegree(self):
        print("\nFinding Node(s) with lowest incoming number (> 2)...")
        n = None
        OD = 0
        Dic = {}
        q = self.graph.run\
        ("MATCH(n:Person)<-[:ISCONNECTED]-(p:Person) WITH n, count(*) AS indegree WHERE indegree > 2 RETURN n, indegree ORDER BY indegree ASC LIMIT 50")\
        .data()
        for i in q:
            if n == None:
                n = str(i["n"]).split("\'")[1]
                OD = i["indegree"]
                Dic[n] = OD
            elif OD == i["indegree"]:
                n = str(i["n"]).split("\'")[1]
                OD = i["indegree"]
                Dic[n] = OD
            else:
                break
        for i,j in Dic.items():
            print ("The Node: ",i," has ",j," Incoming relations")
        print("\n")

    def shortestPath(self):
        print ("Type the name of the two nodes to find the shortest path between them")
        n1 = input('Source node: ')
        n2 = input('Destination node: ')
        print("\nFinding shortest path between source node ",n1," and destination node ",n2)
        query = "MATCH (cs:Person { name: \"%s\" }),(ms:Person { name: \"%s\" }), p = shortestPath((cs)-[*]-(ms)) WHERE length(p)> 1 RETURN p" %(str(n1),str(n2))
        q = self.graph.run(query).data()
        if len(q) == 0:
            print ("No relation between the nodes provided or the nodes are not exist...\
                   \nProgram will exit")
            sys.exit(1)
        print ("\nThe shortest path would be: ")
        path = str(q[0]["p"]).replace("(", "@").replace(")","@").split("@")
        BestPath = []
        for i in path:
            if (i.strip()) and (not ("]->" in i or "<-[" in i)):
                BestPath.append(i)
        print (' --> '.join(BestPath))

    def deleteWholeGraph(self):
        self.graph.run("MATCH (n) DETACH DELETE n").data()

def main():
    print ("\n(((((((((WELCOME TO neo4j SOLUTION TECHNOLOGY)))))))))")
    print ("Neo4j database connection to http://localhost:7474")
    USER = input('username: ')
    PASS = input('password: ')
    app_run = neo4j_solution(USER,PASS)
    app_run.neo4j_connection()
    app_run.read_file()
    app_run.MaxNode_outInDegree()
    app_run.MinNode_InDegree()
    app_run.shortestPath()

    # uncomment this line to delete the whole graph in the database.
    # app_run.deleteWholeGraph()


if __name__ == "__main__":
    main()
