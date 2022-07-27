# Databricks notebook source
dbutils.widgets.text("input", "")
dbutils.widgets.get("input")
y = getArgument("input")
print("parameter- input:")
print(y)
