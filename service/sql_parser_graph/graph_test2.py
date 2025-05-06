# -*- coding:utf-8 -*-
# CREATED BY: bohuai jiang 
# CREATED ON: 
# LAST MODIFIED ON:
# AIM:
import networkx as nx
import matplotlib.pyplot as plt
import matplotlib as mpl
from service.sql_parser_graph.SQLParser import SQLParser
from service.sql_parser_graph.units import ParseUnit, ParseUnitList
from typing import List, Optional, Dict

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
 select * from tab2 as t1, tab1 as t2 where t1.id = t2.id
 '''

def get_name(unit: ParseUnit) -> str:
    name = unit.name
    as_name = unit.as_name
    type = unit.type
    # if as_name is not None and as_name != 'DUMMY':
    #     if type != 'TAB':
    #         return as_name + "." + name
    #     else:
    #         return as_name
    # else:
    #     return name
    return name


def build_graph(sem_info: SQLParser):
    G = nx.Graph()
    elements = sem_info.elements
    tab_name = []
    # -- add nodes --#
    for unit in elements.by_type['TAB']:
        if '(' and 'dual' not in unit.name:
            name = get_name(unit)
            tab_name.append(name)
            G.add_node(name)
    # -- add edges --#
    for opt in elements.by_type['OPT']:
        is_join_col = False
        if len(opt.edges) == 2:
            is_join_col = True
            for v in opt.edges:
                col = elements.by_id[v]
                u_type = col.type
                if u_type in ['VALUE', 'STRUCT']:
                    is_join_col = False
                    break

        if is_join_col:
            col_ids = elements.find_root(opt)
            tab_names = []
            for id in col_ids:
                each_col = elements.by_id[id]
                try:
                    tab_name = [get_name(elements.by_id[i]) for i in elements.find_tab(each_col, tab_only=True)][0]
                    tab_names.append(tab_name)
                except:
                    continue
            weight = 1
            rest = 'full connect('
            if opt.name != '=':
                weight = 0.5
                rest = 'half connect('
            if len(tab_names) >= 2:
                if tab_names[0] != tab_names[1]:
                    G.add_edge(tab_names[0], tab_names[1], weight=weight, method=rest+opt.name+')')

    return G

def name_edges(pos:dict, G: nx.Graph)-> None:
    edges = G.edges(data=True)
    for v,u,att in edges:
        x = (pos[v][0] + pos[u][0])*0.5
        y = (pos[v][1] + pos[u][1])*0.5
        name = att['method']
        plt.text(x, y + 0.01, name)


if __name__ == "__main__":
    parse = SQLParser(sql)
    G = build_graph(parse)
    elarge = [(u, v) for (u, v, d) in G.edges(data=True) if d['weight'] > 0.5]
    esmall = [(u, v) for (u, v, d) in G.edges(data=True) if d['weight'] <= 0.5]
    pos = nx.circular_layout(G)

    # nodes
    nx.draw_networkx_nodes(G, pos, node_size=1000, alpha=0.5)

    # edges
    nx.draw_networkx_edges(G, pos, edgelist=elarge,
                           width=4, edge_color='b',alpha=0.5)
    nx.draw_networkx_edges(G, pos, edgelist=esmall,
                           width=4, edge_color='b',alpha=0.5
                           , style='dashed')

    # labels
    nx.draw_networkx_labels(G, pos, font_size=10, font_family='sans-serif')
    name_edges(pos, G)
    plt.axis('off')
    plt.show()
