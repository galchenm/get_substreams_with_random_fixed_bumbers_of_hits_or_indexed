#!/usr/bin/env python3
# coding: utf8

"""
python3 main.py -i stream.stream -ind True -n N
"""


import os
import sys

import argparse


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
    parser.add_argument('-n','--n', type=int, help="Fixed number of hits or indexed patterns")
    parser.add_argument('-hi','--hi', default=False, action="store", help="Use this flag if you want to grep hits")
    parser.add_argument('-ind','--ind', default=False, action="store", help="Use this flag if you want to grep indexed patterns")
    return parser.parse_args()


def getting_required_number_hits_or_indexed(input_stream, num):
    
    names_of_files = {}
    for i in range(num):
        name_of_block = input_stream.split('.')[0] + f'-{i}.stream'
        names_of_files[i] = name_of_block
    
    i = 0
    
    with open(input_stream, 'r') as stream:
        reading_chunk = False
        
        for line in stream:
            if line.strip() == '----- Begin chunk -----':
                reading_chunk = True
                chunk = line
                i += 1

            elif line.strip() == '----- End chunk -----':
                reading_chunk = False
                chunk += line
                out = open(names_of_files[i%num],'a+')
                out.write(chunk)
                out.close()

            elif reading_chunk:
                chunk += line
            else:
                for j in range(num):
                    out = open(names_of_files[j],'a+')
                    out.write(line)
                    out.close()

    print('FINISH')


def parsing_stream_hits(input_stream, output_stream):
    out2 = open(output_stream, 'w')
    
    total_hits = 0

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
                total_hits += 1


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
    return total_hits


def parsing_stream_indexed_patterns(input_stream, output_stream):
    out2 = open(output_stream, 'w')
    
    total_indexed = 0

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
                    total_indexed += 1
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
    return total_indexed


if __name__ == "__main__":
    args = parse_cmdline_args()
    input_list_of_stream = args.f
    required_numbers = args.n
    output_stream = -100000
    if input_list_of_stream is not None:
        path = os.path.dirname(input_list_of_stream)
        with open(input_list_of_stream, 'r') as f:
            for input_stream in f:
                if len(input_stream.strip()) != 0:
                    input_stream2 = os.path.basename(input_stream.strip())
                    
                    if args.hi:
                        output_stream = os.path.join(path, input_stream2.split('.')[0]+"_all_hits.stream")
                        total_number = parsing_stream_hits(input_stream.strip(), output_stream)
                    elif args.ind:
                        output_stream = os.path.join(path, input_stream2.split('.')[0]+"_all_indexed.stream")
                        total_number = parsing_stream_indexed_patterns(input_stream.strip(), output_stream)
                    else:
                        print('Warning!')
                        
                else:
                    continue
    elif args.i is not None:
        if args.hi:
            if args.o:
                output_stream = args.o
                total_number = parsing_stream_hits(args.i, output_stream)
            else:
                output_stream = args.i.split('.')[0]+"_all_hits.stream"
                total_number = parsing_stream_hits(args.i, output_stream)
        elif args.ind:
            if args.o:
                output_stream = args.o
                total_number = parsing_stream_indexed_patterns(args.i, output_stream)
            else:
                output_stream = args.i.split('.')[0]+"_all_indexed.stream"
                total_number = parsing_stream_indexed_patterns(args.i, output_stream)
        else:
            print('Warning')
            
    if required_numbers > total_number:
        print(f'Reconsider your value, because streams contains only {total_number} of hits/indexed patterns')
    else:
        print(f'Your final stream contains only {total_number} of hits/indexed patterns')
        num = round(total_number / required_numbers)
        print(f'This stream will be devided randomly into round({total_number} / {required_numbers}) = {num} streams')
        getting_required_number_hits_or_indexed(output_stream, num)
        