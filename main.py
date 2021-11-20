import sys
from src.some_storage_library import SomeStorageLibrary
import os
import csv

sys.path.append('src/')
source_d = 'SOURCEDATA.txt'
source_c = 'SOURCECOLUMNS.txt'
path_f = 'data/source/'

def header_f(column_f,f_path):
	column_file = open(f_path + column_f, "r")
	lista_aux = []
	header = []
	for line in column_file:
		stripped_line = line.strip()
		line_list = stripped_line.split('|')
		lista_aux.append([int(line_list[0]),line_list[1]])
	column_file.close()
	sorted_list = sorted(lista_aux, key=lambda x: x[0])
	for element in sorted_list:
		header.append(element[1])
	return header

def data_file(data_f,f_path,head):
	source_file = open(f_path + data_f, "r")
	with open ('final_file.csv', 'w') as f: 
		write = csv.writer(f) 
		write.writerow(head) 
		for line in source_file:
			stripped_line = line.strip()
			row = stripped_line.split('|')
			write.writerow(row) 
		source_file.close()
	return f

if __name__ == '__main__':
	print('Beginning the ETL process...')
	header_file = header_f(source_c,path_f)
	file = data_file(source_d,path_f,header_file)
	f1 = SomeStorageLibrary()
	f1.load_csv('final_file.csv')
