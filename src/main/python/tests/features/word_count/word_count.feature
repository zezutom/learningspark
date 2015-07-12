Feature: Apache Spark makes it possible to easily count words in text files

  Background:
    Given I am using Apache Spark
    Given I am using the WordCount

  Scenario: Calculate words in an empty set
    Given I provide no content
    Then I should see no result