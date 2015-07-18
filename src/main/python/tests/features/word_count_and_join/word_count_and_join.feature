Feature: Apache Spark makes it possible to easily count words in text files

  Background:
    Given I am using Apache Spark
    And I am using the WordCountAndJoin

  Scenario: Calculate occurrences of "Spark"
    Given I provide two different texts about "a spark" and "Apache Spark"
    Then I should see the expected word count per text