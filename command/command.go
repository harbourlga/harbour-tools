package command

import (
	"github.com/urfave/cli"
	"harbour-tools/api/mysqloperate"
)

var (
	BuildVersion               = "0.1"
	Commands      = []cli.Command{
		{
			Name:  "sql",
			Usage: "数据库表相关的处理",
            Flags: []cli.Flag{

			},
			Subcommands: []cli.Command{
						{
							Name: "TwoTableReflect",
							Usage: "DB里两张表映射，将原始表的数据导入目标表",
							Flags: []cli.Flag{
								cli.StringFlag{
									Name: "s",
									Usage: "原始表与目标表的DB是否相同，相同填y。相同后面DB参数只需填原始表DB和table+目标表table",
								},
								cli.StringFlag{
									Name: "oh",
									Usage: "原始表DB host",
								},
								cli.StringFlag{
									Name: "th",
									Usage: "目标表DB host",
									Required: false,
								},
								cli.StringFlag{
									Name: "op",
									Usage: "原始表DB port",
								},
								cli.StringFlag{
									Name: "tp",
									Usage: "目标表DB port",
									Required: false,
								},
								cli.StringFlag{
									Name: "opw",
									Usage: "原始表DB password",
								},
								cli.StringFlag{
									Name: "tpw",
									Usage: "目标表DB password",
									Required: false,
								},
								cli.StringFlag{
									Name: "odn",
									Usage: "原始表DB name",
								},
								cli.StringFlag{
									Name: "tdn",
									Usage: "目标表DB name",
									Required: false,
								},
								cli.StringFlag{
									Name: "ot",
									Usage: "原始表DB table",
								},
								cli.StringFlag{
									Name: "tt",
									Usage: "目标表DB table",
								},
							},
							Action: mysqloperate.ReflectTwoSqlTable,
						},
			},

	},
	}
	Author = []cli.Author{
		{
			Name: "harbour",
			Email: "315874482@qq.com",
		},
	}
)



