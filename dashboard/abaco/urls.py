from django.conf.urls import url
from . import views

urlpatterns = [
    url(r'^request_access$', views.request_access, name='request_access'),
    url(r'^reactors', views.reactors, name='reactors'),
    url(r'^run$', views.run, name='run'),
    url(r'^help$', views.help, name='help'),
    url(r'^register', views.register, name='register'),
    url(r'^login$', views.login, name='login'),
    url(r'^logout$', views.logout, name='logout'),
    url(r'^actor$', views.actors, name='actors'),
    url(r'^$', views.login, name='login'),
    url(r'^worker$',views.worker,name='worker'),
    url(r'^execution$',views.execution,name='execution')
]
