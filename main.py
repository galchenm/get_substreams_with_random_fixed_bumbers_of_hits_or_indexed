#!/usr/bin/env python3
# coding: utf8

"""
python3 main.py -i stream.stream -ind True -n N
"""


import os
import sys
import subprocess
import argparse
import numpy as np
import shutil

class CustomFormatter(argparse.RawDescriptionHelpFormatter,
                      argparse.ArgumentDefaultsHelpFormatter):
    pass

def parse_cmdline_args():
    parser = argparse.ArgumentParser(
        description=sys.modules[__name__].__doc__,
        formatter_class=CustomFormatter)
    parser.add_argument('-i','--i', type=str, help="Input stream file")
    parser.add_argument('-o','--o', type=str, help="Ouput stream file")
    parser.add_argument('-f','--f', type=str, help="Input file with list of streams file")
    parser.add_argument('-n','--n', type=int, default=1, help="Fixed number of hits or indexed patterns")
    parser.add_argument('-hi','--hi', default=False, action="store", help="Use this flag if you want to grep hits")
    parser.add_argument('-ind','--ind', default=False, action="store", help="Use this flag if you want to grep indexed patterns")
    parser.add_argument('-wo','--wo', default=False, action="store", help="Use this flag if you do not want to grep indexed patterns or patterns with hits and you just need to get random part from the single stream")
    return parser.parse_args()



def getting_required_number_hits_or_indexed(input_stream, ind):
    
    name_of_block = input_stream.split('.')[0] + f'-with-{len(ind)}-preselected.stream'
    
    i = 0
    
    with open(input_stream, 'r') as stream:
        reading_chunk = False
        
        for line in stream:
            if line.strip() == '----- Begin chunk -----':
                reading_chunk = True
                chunk = line
                
                
            elif line.strip() == '----- End chunk -----':
                reading_chunk = False
                chunk += line
                if i in ind:
                    out = open(name_of_block,'a+')
                    out.write(chunk)
                    out.close()
                i += 1
            elif reading_chunk:
                chunk += line
            else:

                out = open(name_of_block,'a+')
                out.write(line)
                out.close()
    
    print(i == len(ind))
    print('FINISH')

def parsing_stream_hits(input_stream, output_stream):
    out2 = open(output_stream, 'w')
    


    with open(input_stream, 'r') as stream:
        reading_chunk = False
        reading_geometry = False
        found_hit = False

        for line in stream:
            if line.startswith('CrystFEL stream format'):
                chunk_geom = line
                reading_geometry = True
            elif line.strip() == '----- End geometry file -----':  #
                reading_geometry = False
                chunk_geom += line
                out2.write(chunk_geom)

            elif line.strip() == '----- Begin chunk -----':
                reading_chunk = True
                chunk = line

            elif line.startswith('Image filename:'):
                chunk += line
                
            elif line.startswith('Event:'):
                serial_number = line.split('//')[-1]
                chunk += line     

            elif line.startswith('hit = 1'): #none
                found_hit = True 
                chunk += line


            elif line.strip() == '----- End chunk -----':
                reading_chunk = False
                chunk += line
                if found_hit:
                    out2.write(chunk)
                found_hit = False

            elif reading_chunk:
                chunk += line

            elif reading_geometry:
                chunk_geom += line
    out2.close()



def parsing_stream_indexed_patterns(input_stream, output_stream):
    out2 = open(output_stream, 'w')
    

    with open(input_stream, 'r') as stream:
        reading_chunk = False
        reading_geometry = False
        found_indexed = False

        for line in stream:
            if line.startswith('CrystFEL stream format'):
                chunk_geom = line
                reading_geometry = True
            elif line.strip() == '----- End geometry file -----':  #
                reading_geometry = False
                chunk_geom += line
                out2.write(chunk_geom)

            elif line.strip() == '----- Begin chunk -----':
                reading_chunk = True
                chunk = line

            elif line.startswith('Image filename:'):
                chunk += line
                
            elif line.startswith('Event:'):
                serial_number = line.split('//')[-1]
                chunk += line     

            elif line.startswith('indexed_by ='): #none
                indexed_by = line.strip().split(' = ')[-1]
                
                if indexed_by != 'none':
                    found_indexed = True
                chunk += line

            elif line.strip() == '----- End chunk -----':
                reading_chunk = False
                chunk += line
                if found_indexed:
                    out2.write(chunk)
                found_indexed = False

            elif reading_chunk:
                chunk += line

            elif reading_geometry:
                chunk_geom += line
    out2.close()


def calculate_the_total_number_of_indexed_patterns_and_hits(stream, hits_flag=True):
    try:
        chunks = int(subprocess.check_output(['grep', '-c', 'Image filename',stream]).decode('utf-8').strip().split('\n')[0]) #len(res_hits)
    except subprocess.CalledProcessError:
        chunks = 0

    try:
        res_hits = subprocess.check_output(['grep', '-rc', 'hit = 1',stream]).decode('utf-8').strip().split('\n')
        hits = int(res_hits[0])
    except subprocess.CalledProcessError:
        hits = 0

    try:
        res_none_indexed_patterns = subprocess.check_output(['grep', '-rc', 'indexed_by = none',stream]).decode('utf-8').strip().split('\n')
        none_indexed_patterns = int(res_none_indexed_patterns[0])
    except subprocess.CalledProcessError:
        none_indexed_patterns = 0

    indexed_patterns = chunks - none_indexed_patterns
    
    if hits_flag:
        return hits, chunks
    else:
        return indexed_patterns, chunks


def picking_up_number(input_stream, total_number, required_numbers):
    ind = np.random.choice(total_number+1, size=required_numbers, replace=False)
    print(len(ind))

    if len(set(ind)) == len(ind):
        getting_required_number_hits_or_indexed(input_stream, ind)
    else:
        print('Warning! something odd with randomizer!')

if __name__ == "__main__":
    args = parse_cmdline_args()
    input_list_of_stream = args.f
    required_numbers = args.n
    output_stream = ''
    total_number = 0
    if input_list_of_stream is not None:
        path = os.path.dirname(input_list_of_stream)
        with open(input_list_of_stream, 'r') as f:
            for input_stream in f:
                total_number, chunks = calculate_the_total_number_of_indexed_patterns_and_hits(input_stream, args.hi)
                if len(input_stream.strip()) != 0:
                    input_stream2 = os.path.basename(input_stream.strip())
                    
                    if args.hi:
                        output_stream = os.path.join(path, input_stream2.split('.')[0]+"_all_hits.stream")
                        parsing_stream_hits(input_stream, output_stream)
                    elif args.ind:
                        output_stream = os.path.join(path, input_stream2.split('.')[0]+"_all_indexed.stream")
                        parsing_stream_indexed_patterns(input_stream, output_stream)
                    elif args.wo:
                        total_number = chunks
                        output_stream = input_stream
                    else:
                        print('Warning!')
                    
                    
                    if required_numbers > total_number:
                        print(f'Reconsider your value, because streams contains only {total_number} of hits/indexed patterns/chunks')
                    elif required_numbers == total_number:
                        print(f'Your stream contains exactly {total_number} hits/indexed patterns/chunks of interest')
                        shutil.copyfile(output_stream, output_stream.split('.')[0] + f'-with-{total_number}-preselected.stream')        
                    else:
                        print(f'Your final stream contains only {total_number} of hits/indexed patterns/chunks')
                        picking_up_number(output_stream, total_number, required_numbers)        
                
                else:
                    continue
                
    elif args.i is not None:
        input_stream = args.i
        total_number, chunks = calculate_the_total_number_of_indexed_patterns_and_hits(input_stream, args.hi)
        if args.hi:
            if args.o:
                output_stream = args.o
            else:
                output_stream = input_stream.split('.')[0]+"_all_hits.stream"
            parsing_stream_hits(input_stream, output_stream)
        elif args.ind:
            if args.o:
                output_stream = args.o
            else:
                output_stream = input_stream.split('.')[0]+"_all_indexed.stream"
            parsing_stream_indexed_patterns(input_stream, output_stream)
        elif args.wo:
            total_number = chunks
            output_stream = input_stream
        else:
            print('Warning')
        if required_numbers > total_number:
            print(f'Reconsider your value, because streams contains only {total_number} of hits/indexed patterns/chunks')
        elif required_numbers == total_number:
            print(f'Your stream contains exactly {total_number} hits/indexed patterns/chunks of interest')
            shutil.copyfile(output_stream, output_stream.split('.')[0] + f'-with-{total_number}-preselected.stream')
        else:
            print(f'Your stream contains only {total_number} of hits/indexed patterns/chunks')
            picking_up_number(output_stream, total_number, required_numbers) 