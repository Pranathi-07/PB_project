from django.shortcuts import render
import sys
from subprocess import run, PIPE

# Create your views here.
def index(request):
    return render(request, 'dataanalysis/index.html')


def execute(request, id):
    result = run([sys.executable, 'C:\\Users\\gudiy\\Desktop\\PB-Visualizations\\pb_phase2.py'], shell=False, stdout=PIPE)
    print(result)
    return render(request, 'dataanalysis/output.html')
