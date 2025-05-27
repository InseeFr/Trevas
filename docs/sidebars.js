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
			items: [
				'introduction/index-introduction',
				{
					type: 'category',
					label: 'Modules',
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
			items: [
				{
					type: 'category',
					label: 'VTL playground',
					items: [
						{
							type: 'category',
							label: 'Learn VTL',
							items: [
								'user-guide/vtl/index-vtl',
								{
									type: 'category',
									label: 'Sas vs VTL',
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
			items: [
				'developer-guide/index-developer-guide',
				'developer-guide/developer-javadoc',
				{
					type: 'category',
					label: 'Basic mode',
					items: [
						'developer-guide/basic-mode/index-basic-mode',
						{
							type: 'category',
							label: 'Data sources',
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
					items: [
						'developer-guide/spark-mode/index-spark-mode',
						{
							type: 'category',
							label: 'Data sources',
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
			items: ['administrator-guide/grammar/index-grammar'],
		},
	],
};
