# -*- coding:utf-8 -*-
# CREATED BY: bohuai jiang 
# CREATED ON: 
# LAST MODIFIED ON:
# AIM:


import networkx as nx
import matplotlib.pyplot as plt
import matplotlib as mpl
from service.sql_parser_graph.SQLParser import SQLParser
from service.sql_parser_graph.units import ParseUnit
from typing import List, Optional, Dict
import gzip

sql = '''
select count(aty.id)
from act_activity aty, act_audit_flow afl
where afl.audit_flow_status = 0 and (aty.status = 11 or aty.status = 1) and aty.id = afl.activity_id and afl.id in (
	select unique(ast.audit_flow_id)
	from act_audit_status ast
	where ast.audit_status_type = 0 and 
		( 
		ast.auditor = :P_auditor or ast.auditor in 
			(
			slelect pri.mandatary
			from act_act_audit_principal pri
			where pri.is_valid = '1' and pri.start_at <= sysdate
			and pri.end_at >= sysdate
			)
		and ast.audit_type =0
		)
	)
	'''

sql = '''
 select * from SAMPLE_DATA as i where i.id = :P_ID and i.partition_key = :P_PARKEY
 '''


parse = SQLParser(sql)
print(parse.get_table_name())
elements = parse.elements.by_id


def get_name(unit: ParseUnit) -> str:
    name = unit.name
    as_name = unit.as_name
    type = unit.type
    if as_name is not None and as_name != 'DUMMY':
        if type != 'TAB':
            return as_name + "." + name
        else:
            return as_name
    else:
        return name


def add_each_id(elements: Dict[int, ParseUnit]) -> nx.Graph:
    valid_type = ['STRUCT', 'FUNC']
    G = nx.DiGraph()
    G.node_size = dict()
    G.color = dict()
    G.name = dict()

    tab_name = dict()
    # pos = nx.layout.spring_layout(G)
    for id in elements:
        node_size = 100
        color = 'b'
        name = get_name(elements[id])
        edges = elements[id].edges
        type = elements[id].type
        duplicate_id = None
        if type in valid_type:
            continue
        if type == 'TAB':
            if '(' in name:
                continue
            else:
                node_size = 500
                color = 'r'
                if name not in tab_name.keys():
                    tab_name[name] = id
                else:
                    duplicate_id = tab_name[name]
        if duplicate_id is None:
            G.add_node(id)
            G.node_size[id] = node_size
            G.color[id] = color
            G.name[id] = name
        for e in edges:
            e_name = get_name(elements[e])
            type = elements[e].type
            if type in valid_type:
                continue
            if type == 'TAB':
                if '(' in e_name:
                    continue
            if duplicate_id is None:
                G.add_edge(id, e)
            else:
                G.add_edge(duplicate_id, e)
    return G


def add_each(elements: Dict[int, ParseUnit]) -> nx.Graph:
    valid_type = ['STRUCT', 'SUB', 'FUNC', 'OPT', 'VALUE']
    G = nx.DiGraph()
    G.node_size = dict()
    G.color = dict()
    # pos = nx.layout.spring_layout(G)
    for id in elements:
        node_size = 100
        color = 'b'
        name = get_name(elements[id])
        edges = elements[id].edges
        type = elements[id].type
        if type in valid_type:
            continue
        if type == 'TAB':
            if '(' in name:
                continue
            else:
                node_size = 500
                color = 'r'
        G.add_node(name)
        G.node_size[name] = node_size
        G.color[name] = color
        for e in edges:
            e_name = get_name(elements[e])
            type = elements[e].type
            if type in valid_type:
                continue
            if type == 'TAB':
                if '(' in e_name:
                    continue
            G.add_edge(name, e_name)
    return G


plt.figure(figsize=(8, 8))
G = add_each_id(elements)
node_color = [8 for v in G]
# nx.draw_networkx_edges(G, edge_color='b', width=4, aplpha= 0.5)
# pos = nx.layout.spring_layout(G)
pos = nx.shell_layout(G)
nx.draw_networkx_edges(G, pos, edge_color='b', width=4, alpha=0.5)
#nx.draw_networkx_nodes(G, pos, node_size=[G.node_size[v] for v in G], node_color=[G.color[v] for v in G], alpha=0.4)
# nx.draw_networkx_labels(G, pos, fontsize=14)
# nx.draw(G,
#         node_color= 'k',
#         node_size = [G.node_size[v] for v in G],
#         edge_color = 'b',
#         width = 5,
#         alpha = 0.5,
#         font_size = 8)
for i, node in enumerate(G.nodes):
    x, y = pos[node]
    name = G.name[node]
    plt.text(x, y + 0.01, name)
plt.show()
