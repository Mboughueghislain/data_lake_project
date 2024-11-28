from django.urls import path, re_path  # Utilisez re_path pour les expressions régulières
from . import views

urlpatterns = [
    # Utilisation de re_path pour accepter des chemins complexes avec des caractères spéciaux
    re_path(r'^api/parquet/(?P<file_path>.+)/$', views.get_parquet_data, name='get_parquet_data'),
]
