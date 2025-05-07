"""Utility functions for matching with logmap"""
import shlex
import subprocess
import os 

def build_image(tag:str = '0.01'):
    """build the logmap image with a specfied tag"""
    cmd = f'docker build -f mapnet/logmap/container/Dockerfile ./ -t logmap:{tag}'
    print(f"running, {cmd}")
    subprocess.run(shlex.split(cmd))

def get_results_path():
    """define a path to write results for logmap"""
    output_path = os.path.join(os.getcwd(), 'mapnet', 'logmap', 'output')
    return output_path 

def run_container(target_onto_file:str, source_onto_file:str, 
        output_path:str = None, resources_path:str = 'resources/', 
        ext:str= 'obo', tag:str = '0.01',):
    ##  define base of cmd
    cmd = shlex.split('docker run --rm')
    # Mount where output will be written
    output_path = output_path or get_results_path()
    cmd += ['-v', f'{shlex.quote(output_path)}:/package/output']
    
    # Mount directory with ontologies
    cmd += ['-v', f'{shlex.quote(resources_path)}:/package/resources/']
    
    # specify the image and tag
    cmd += [f"logmap:{shlex.quote(tag)}"]
    # Define Java command arguments as a list
    java_cmd = [
        'java', '-jar',
        '-Xmx32g',
        '--add-opens', 'java.base/java.lang=ALL-UNNAMED',
        '/package/logmap/logmap-matcher-4.0.jar',
        'MATCHER',
        f'file:///package/resources/{shlex.quote(target_onto_file)}',
        f'file:///package/resources/{shlex.quote(source_onto_file)}',
        '/package/output/',
        'false'  # classify input ontology as well as map 
    ]
    # java_cmd = ['ls', './resources']
    # Add the Java command to the Docker command
    cmd += ['sh', '-c' , shlex.join(java_cmd)]
    print(shlex.join(cmd))
    # print(cmd)
    # run the command 
    subprocess.run(cmd)

