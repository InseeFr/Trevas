module.exports = {
	docs: [
		{
			type: 'link',
			label: 'Home',
			href: '/',
		},
		{
			type: 'category',
			label: 'Introduction',
			key: 'introduction',
			items: [
				'introduction/index-introduction',
				{
					type: 'category',
					label: 'Modules',
					key: 'introduction-modules',
					items: [
						'introduction/modules/index-modules',
						'introduction/modules/engine',
						'introduction/modules/parser',
						'introduction/modules/spark',
						'introduction/modules/model',
						'introduction/modules/jdbc',
						'introduction/modules/jackson',
						'introduction/modules/csv',
						'introduction/modules/sdmx',
						'introduction/modules/prov',
					],
				},
				{
					type: 'category',
					label: 'Releases',
					key: 'introduction-releases',
					items: [
						'introduction/releases/index-releases',
						'introduction/releases/1.x.x',
					],
				},
			],
		},
		{
			type: 'category',
			label: 'User guide',
			key: 'user-guide',
			items: [
				{
					type: 'category',
					label: 'VTL playground',
					key: 'user-guide-vtl-playground',
					items: [
						{
							type: 'category',
							label: 'Learn VTL',
							key: 'user-guide-learn-vtl',
							items: [
								'user-guide/vtl/index-vtl',
								{
									type: 'category',
									label: 'Sas vs VTL',
									key: 'user-guide-sas-vtl',
									items: [
										'user-guide/vtl/sas-vtl/index-sas-vtl',
										'user-guide/vtl/sas-vtl/keep',
										'user-guide/vtl/sas-vtl/drop',
										'user-guide/vtl/sas-vtl/rename',
									],
								},
							],
						},
						{
							type: 'category',
							label: 'Client apps',
							key: 'user-guide-client-apps',
							items: [
								'user-guide/vtl/client-apps/index-client-apps',
								'user-guide/vtl/client-apps/jupyter/index-jupyter',
								'user-guide/vtl/client-apps/lab/index-lab',
							],
						},
					],
				},
				{
					type: 'category',
					label: 'Coverage',
					key: 'user-guide-coverage',
					items: [
						'user-guide/coverage/index-coverage',
						'user-guide/coverage/general-operators',
						'user-guide/coverage/join-operators',
						'user-guide/coverage/string-operators',
						'user-guide/coverage/numeric-operators',
						'user-guide/coverage/comparison-operators',
						'user-guide/coverage/boolean-operators',
						'user-guide/coverage/time-operators',
						'user-guide/coverage/set-operators',
						'user-guide/coverage/hierarchical-aggregation',
						'user-guide/coverage/aggregate-operators',
						'user-guide/coverage/analytic-operators',
						'user-guide/coverage/data-validation-operators',
						'user-guide/coverage/conditional-operators',
						'user-guide/coverage/clause-operators',
					],
				},
			],
		},
		{
			type: 'category',
			label: 'Developer guide',
			key: 'developer-guide',
			items: [
				'developer-guide/index-developer-guide',
				'developer-guide/developer-javadoc',
				'developer-guide/dag',
				{
					type: 'category',
					label: 'Basic mode',
					key: 'developer-guide-basic-mode',
					items: [
						'developer-guide/basic-mode/index-basic-mode',
						{
							type: 'category',
							label: 'Data sources',
							key: 'basic-mode-data-sources',
							items: [
								'developer-guide/basic-mode/data-sources/index-data-sources',
								'developer-guide/basic-mode/data-sources/jdbc',
								'developer-guide/basic-mode/data-sources/json',
								'developer-guide/basic-mode/data-sources/others',
							],
						},
					],
				},
				{
					type: 'category',
					label: 'Spark mode',
					key: 'developer-guide-spark-mode',
					items: [
						'developer-guide/spark-mode/index-spark-mode',
						{
							type: 'category',
							label: 'Data sources',
							key: 'spark-mode-data-sources',
							items: [
								'developer-guide/spark-mode/data-sources/index-data-sources',
								'developer-guide/spark-mode/data-sources/csv',
								'developer-guide/spark-mode/data-sources/jdbc',
								'developer-guide/spark-mode/data-sources/parquet',
								'developer-guide/spark-mode/data-sources/sdmx',
								'developer-guide/spark-mode/data-sources/others',
							],
						},
					],
				},
				'developer-guide/provenance',
			],
		},
		{
			type: 'category',
			label: 'Administrator guide',
			key: 'administrator-guide',
			items: ['administrator-guide/grammar/index-grammar'],
		},
	],
};
