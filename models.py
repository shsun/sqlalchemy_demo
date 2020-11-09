# coding: utf-8
from sqlalchemy import Column, MetaData, String, Table

metadata = MetaData()


t_team = Table(
    'team', metadata,
    Column('name', String(255))
)


t_team2 = Table(
    'team2', metadata,
    Column('name', String(255))
)
