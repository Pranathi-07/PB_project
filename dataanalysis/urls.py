from django.urls import path
from .views import index, execute

urlpatterns = [
    path('', index, name='index'),
    path('execute/<str:id>',execute, name='execute')
]
