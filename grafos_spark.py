# 9.4.18: Grafos
#        E
#      /   \
#    B      G
#  /   \   / \
#A       F    H
#  \        /
#    C -- D -- I

#Formas de representarlo:

#- Lista de aristas:
#(A,B),(B,E),(B,F),(A,C),(E,G),(F,G),(C,D),(D,H),(D,I),(G,H)

#- Lista de adyacencias
#(A,[B,C]),(B,[A,E,F]),(C,[A,D]),(D,[C,H,I]),(E,[B,G]),(F,[B,G]),(G,[H,E,F]),(H,[G,D]),(I,[D])


#1) Bacon numbers
#En el grafo del ejemplo asigno a A como el elemento Bacon y quiero averiguar el número Bacon de cualquier otro nodo, es decir, el camino mínimo de A a cualquier nodo.

#Aplicamos BFS:

# genero lista de cuádruplas con: (nodo, adyacencias, status, distancia)
# status inicial es '-' y pasará a 'p' una vez que el nodo haya sido procesado
# distancia inicial es infinito para todos los nodos
ady = aristas.flatMap(lambda x: [(x[0],x[1]),(x[1],x[0])])\
	.groupByKey().map(lambda x: (x[0],list(set(x[1])),'-',infinito)) 

# asigno distancia cero y status 'p' al nodo que me interesa
ady = ady.map(lambda x:(x[0],x[1],'p',0) if x[0] == 'A') # otra forma: filter map

for i in range(0,15):
	ady = ady.flatMap(proc_node).cache()
    ady = ady.reduceByKey(reduce_two_nodes).cache()

def proc_node(node):
    if node[2] = 'p' # processing
	result = []
	node[2] = 'd' # done 
	result.append(node)
	for child in node[1]:
		child = [child, [], 'p', child[3] + 1]
		result.append(child)
	return result


def reduce_two_nodes(x, y):
	if (len(y[1]) > len(x[1])):
		x[1] = y[1]
	x[2] = greater_status(x[2], y[2])
	x[3] = min([x[3], y[3]])
	return x