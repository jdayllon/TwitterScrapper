#!/bin/bash
# Index Creation with spanish custom analyzer
echo -e "\n Index Deletion \n"
curl -X DELETE "localhost:9200/twitter" -H 'Content-Type: application/json'
echo -e "\n Index Creation \n"
curl -X PUT "localhost:9200/twitter" -H 'Content-Type: application/json' -d '@index_status_es.json'
echo -e "\n Mapping Creation \n"
# Mapping Creation with spanish custom analyzer
curl -X PUT "localhost:9200/twitter/_mapping/status?include_type_name=true" -H 'Content-Type: application/json' -d @mapping_status_es.json
#curl -X PUT "localhost:9200/twitter/_mapping/status?include_type_name=true" -H 'Content-Type: application/json' -d'
# {
#      "properties": {
#        "created_at": {
#          "type": "date",
#          "fields": {
#            "keyword": {
#              "type": "keyword",
#              "ignore_above": 256
#            }
#          }
#        },
#        "favorite_count": {
#          "type": "long"
#        },
#        "hashtags": {
#          "properties": {
#            "text": {
#              "type": "text",
#              "fields": {
#                "keyword": {
#                  "type": "keyword",
#                  "ignore_above": 256
#                }
#              },
#              "analyzer": "rebuilt_spanish"
#            }
#          }
#        },
#        "id": {
#          "type": "long"
#        },
#        "id_str": {
#          "type": "text",
#          "fields": {
#            "keyword": {
#              "type": "keyword",
#              "ignore_above": 256
#            }
#          }
#        },
#        "in_reply_to_screen_name": {
#          "type": "text",
#          "fields": {
#            "keyword": {
#              "type": "keyword",
#              "ignore_above": 256
#            }
#          }
#        },
#        "in_reply_to_status_id": {
#          "type": "long"
#        },
#        "in_reply_to_user_id": {
#          "type": "long"
#        },
#        "lang": {
#          "type": "text",
#          "fields": {
#            "keyword": {
#              "type": "keyword",
#              "ignore_above": 256
#            }
#          }
#        },
#        "retweet_count": {
#          "type": "long"
#        },
#        "source": {
#          "type": "text",
#          "fields": {
#            "keyword": {
#              "type": "keyword",
#              "ignore_above": 256
#            }
#          }
#        },
#        "text": {
#          "type": "text",
#          "fields": {
#            "keyword": {
#              "type": "keyword",
#              "ignore_above": 256
#            }
#          },
#          "analyzer": "rebuilt_spanish"
#        },
#        "urls": {
#          "properties": {
#            "expanded_url": {
#              "type": "text",
#              "fields": {
#                "keyword": {
#                  "type": "keyword",
#                  "ignore_above": 256
#                }
#              }
#            },
#            "url": {
#              "type": "text",
#              "fields": {
#                "keyword": {
#                  "type": "keyword",
#                  "ignore_above": 256
#                }
#              }
#            }
#          }
#        },
#        "user": {
#          "properties": {
#            "created_at": {
#              "type": "date",
#              "fields": {
#                "keyword": {
#                  "type": "keyword",
#                  "ignore_above": 256
#                }
#              }
#            },
#            "default_profile": {
#              "type": "boolean"
#            },
#            "description": {
#              "type": "text",
#              "fields": {
#                "keyword": {
#                  "type": "keyword",
#                  "ignore_above": 256
#                }
#              },
#              "analyzer": "rebuilt_spanish"
#            },
#            "favourites_count": {
#              "type": "long"
#            },
#            "followers_count": {
#              "type": "long"
#            },
#            "following": {
#              "type": "boolean"
#            },
#            "friends_count": {
#              "type": "long"
#            },
#            "geo_enabled": {
#              "type": "boolean"
#            },
#            "id": {
#              "type": "long"
#            },
#            "id_str": {
#              "type": "text",
#              "fields": {
#                "keyword": {
#                  "type": "keyword",
#                  "ignore_above": 256
#                }
#              }
#            },
#            "lang": {
#              "type": "text",
#              "fields": {
#                "keyword": {
#                  "type": "keyword",
#                  "ignore_above": 256
#                }
#              }
#            },
#            "listed_count": {
#              "type": "long"
#            },
#            "location": {
#              "type": "text",
#              "fields": {
#                "keyword": {
#                  "type": "keyword",
#                  "ignore_above": 256
#                }
#              },
#              "analyzer": "rebuilt_spanish"
#            },
#            "name": {
#              "type": "text",
#              "fields": {
#                "keyword": {
#                  "type": "keyword",
#                  "ignore_above": 256
#                }
#              },
#              "analyzer": "rebuilt_spanish"
#            },
#            "profile_background_color": {
#              "type": "text",
#              "fields": {
#                "keyword": {
#                  "type": "keyword",
#                  "ignore_above": 256
#                }
#              }
#            },
#            "profile_background_image_url": {
#              "type": "text",
#              "fields": {
#                "keyword": {
#                  "type": "keyword",
#                  "ignore_above": 256
#                }
#              }
#            },
#            "profile_background_image_url_https": {
#              "type": "text",
#              "fields": {
#                "keyword": {
#                  "type": "keyword",
#                  "ignore_above": 256
#                }
#              }
#            },
#            "profile_banner_url": {
#              "type": "text",
#              "fields": {
#                "keyword": {
#                  "type": "keyword",
#                  "ignore_above": 256
#                }
#              }
#            },
#            "profile_image_url": {
#              "type": "text",
#              "fields": {
#                "keyword": {
#                  "type": "keyword",
#                  "ignore_above": 256
#                }
#              }
#            },
#            "profile_image_url_https": {
#              "type": "text",
#              "fields": {
#                "keyword": {
#                  "type": "keyword",
#                  "ignore_above": 256
#                }
#              }
#            },
#            "profile_link_color": {
#              "type": "text",
#              "fields": {
#                "keyword": {
#                  "type": "keyword",
#                  "ignore_above": 256
#                }
#              }
#            },
#            "profile_sidebar_border_color": {
#              "type": "text",
#              "fields": {
#                "keyword": {
#                  "type": "keyword",
#                  "ignore_above": 256
#                }
#              }
#            },
#            "profile_sidebar_fill_color": {
#              "type": "text",
#              "fields": {
#                "keyword": {
#                  "type": "keyword",
#                  "ignore_above": 256
#                }
#              }
#            },
#            "profile_text_color": {
#              "type": "text",
#              "fields": {
#                "keyword": {
#                  "type": "keyword",
#                  "ignore_above": 256
#                }
#              }
#            },
#            "profile_use_background_image": {
#              "type": "boolean"
#            },
#            "screen_name": {
#              "type": "text",
#              "fields": {
#                "keyword": {
#                  "type": "keyword",
#                  "ignore_above": 256
#                }
#              }
#            },
#            "statuses_count": {
#              "type": "long"
#            },
#            "url": {
#              "type": "text",
#              "fields": {
#                "keyword": {
#                  "type": "keyword",
#                  "ignore_above": 256
#                }
#              }
#            },
#            "verified": {
#              "type": "boolean"
#            }
#          }
#        },
#        "user_mentions": {
#          "properties": {
#            "id": {
#              "type": "long"
#            },
#            "id_str": {
#              "type": "text",
#              "fields": {
#                "keyword": {
#                  "type": "keyword",
#                  "ignore_above": 256
#                }
#              }
#            },
#            "name": {
#              "type": "text",
#              "fields": {
#                "keyword": {
#                  "type": "keyword",
#                  "ignore_above": 256
#                }
#              },
#              "analyzer": "rebuilt_spanish"
#            },
#            "screen_name": {
#              "type": "text",
#              "fields": {
#                "keyword": {
#                  "type": "keyword",
#                  "ignore_above": 256
#                }
#              }
#            }
#          }
#        }
#      }
#    }
#'
#

