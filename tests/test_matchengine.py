from matchengine import create_match_tree

f = {
    "and0": [
        {
            "and1": [
                {
                    "g1": {
                        "g1_key": "g1_val"
                    },
                    "c1": {
                        "c1_key": "c1_val"
                    }
                },
                {
                    "or2": [
                        {
                            "g2": {
                                "g2_key": "g2_val"
                            }
                        },
                        {
                            "c2": {
                                "c2_key": "c2_val"
                            }
                        },
                        {
                            "and3": [
                                {
                                    "g3": {
                                        "g3_key": "g3_val"
                                    }
                                },
                                {
                                    "c3": {
                                        "c3_key": "c3_val"
                                    }
                                }
                            ]
                        }
                    ]
                },
                {
                    "or4": [
                        {
                            "g4": {
                                "g4_key": "g4_val"
                            }
                        },
                        {
                            "c4": {
                                "c4_key": "c4_val"
                            }
                        },
                        {
                            "or5": [
                                {
                                    "g5": {
                                        "g5_key": "g5_val"
                                    }
                                },
                                {
                                    "c5": {
                                        "c5_key": "c5_val"
                                    }
                                }
                            ]
                        }
                    ]
                },
                {
                    "and6": [
                        {
                            "g6": {
                                "g6_key": "g6_val"
                            },
                            "c6": {
                                "c6_key": "c6_val"
                            }
                        },
                        {
                            "or7": [
                                {
                                    "g7": {
                                        "g7_key": "g7_val"
                                    }
                                },
                                {
                                    "c7": {
                                        "c7_key": "c7_val"
                                    }
                                }
                            ]
                        },
                        {
                            "and8": [
                                {
                                    "g8": {
                                        "g8_key": "g8_val"
                                    }
                                },
                                {
                                    "c8": {
                                        "c8_key": "c8_val"
                                    }
                                }
                            ]
                        }
                    ]
                }

            ],
            "or9": [
                {
                    "g9": {
                        "g9_key": "g9_val"
                    },
                    "c9": {
                        "c9_key": "c9_val"
                    }
                },
                {
                    "and10": [
                        {
                            "g10": {
                                "g10_key": "g10_val"
                            },
                            "c10": {
                                "c10_key": "c10_val"
                            }
                        },
                        {
                            "and11": [
                                {
                                    "g11": {
                                        "g11_key": "g11_val"
                                    }
                                },
                                {
                                    "c11": {
                                        "c11_key": "c11_val"
                                    }
                                }
                            ]
                        },
                        {
                            "or12": [
                                {
                                    "g12": {
                                        "g12_key": "g12_val"
                                    }
                                },
                                {
                                    "c12": {
                                        "c12_key": "c12_val"
                                    }
                                }
                            ]
                        }
                    ],
                },
                {
                    "or13": [
                        {
                            "g13": {
                                "g13_key": "g13_val"
                            }
                        },
                        {
                            "c13": {
                                "g14_key": "g14_val"
                            }
                        },
                        {
                            "or14": [
                                {
                                    "g14": {
                                        "g14_key": "g14_val"
                                    }
                                },
                                {
                                    "c14": {
                                        "c14_key": "c14_val"
                                    }
                                }
                            ]
                        },
                        {
                            "and15": [
                                {
                                    "g15": {
                                        "g15_key": "g15_val"
                                    }
                                },
                                {
                                    "c15": {
                                        "c15_key": "c15_val"
                                    }
                                }
                            ]
                        }
                    ]
                }
            ]
        }
    ]
}
create_match_tree(f['and0'])