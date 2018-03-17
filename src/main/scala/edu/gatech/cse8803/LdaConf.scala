package edu.gatech.cse8803

case class LdaConf(numTopics: Int, maxIterations: Int, outputTopics: String,outputTopTopicsPerDoc: String,
                   outputTopDocsPerTopic: String, stopwords: String)