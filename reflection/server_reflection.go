package reflection

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"text/template"
	"unicode"
	"unicode/utf8"

	"github.com/ChimeraCoder/gojson"
	jsoniter "github.com/json-iterator/go"
	"github.com/smallnest/rpcx/log"
)

var (
	typeOfError   = reflect.TypeOf((*error)(nil)).Elem()
	typeOfContext = reflect.TypeOf((*context.Context)(nil)).Elem()
)

var json = jsoniter.Config{
	TagKey: "-",
}.Froze()

// ServiceInfo service info.
type ServiceInfo struct {
	Name    string
	PkgPath string
	Methods []*MethodInfo
}

// MethodInfo method info
type MethodInfo struct {
	Name      string
	ReqName   string
	Req       string
	ReplyName string
	Reply     string
}

var siTemplate = `package {{.PkgPath}}

type {{.Name}} struct{}
{{$name := .Name}}
{{range .Methods}}
{{.Req}}
{{.Reply}}
type (s *{{$name}}) {{.Name}}(ctx context.Context, arg *{{.ReqName}}, reply *{{.ReplyName}}) error {
	return nil
}
{{end}}
`

func (si ServiceInfo) String() string {
	tpl := template.Must(template.New("service").Parse(siTemplate))
	var buf bytes.Buffer
	_ = tpl.Execute(&buf, si)
	return buf.String()
}

type Reflection struct {
	Services map[string]*ServiceInfo
}

func New() *Reflection {
	return &Reflection{
		Services: make(map[string]*ServiceInfo),
	}
}

// Register 注册服务
func (r *Reflection) Register(name string, rcvr interface{}, metadata string) error {
	si := &ServiceInfo{}

	val := reflect.ValueOf(rcvr)
	typ := reflect.TypeOf(rcvr)
	vTyp := reflect.Indirect(val).Type()
	// Struct 名称
	si.Name = vTyp.Name()
	// Struct 所在 package 路径
	pkg := vTyp.PkgPath()
	if strings.Index(pkg, ".") > 0 {
		pkg = pkg[strings.LastIndex(pkg, ".")+1:]
	}
	pkg = filepath.Base(pkg)
	pkg = strings.ReplaceAll(pkg, "-", "_")
	si.PkgPath = pkg

	// 遍历 Struct 中所有方法
	for m := 0; m < val.NumMethod(); m++ {
		method := typ.Method(m)
		mtype := method.Type

		if method.PkgPath != "" {
			continue
		}
		if mtype.NumIn() != 4 {
			continue
		}
		// 下面校验方法参数是否符合 rpcx 方法要求
		// First arg must be context.Context
		ctxType := mtype.In(1)
		if !ctxType.Implements(typeOfContext) {
			continue
		}

		// Second arg need not be a pointer.
		argType := mtype.In(2)
		if !isExportedOrBuiltinType(argType) {
			continue
		}
		// Third arg must be a pointer.
		replyType := mtype.In(3)
		if replyType.Kind() != reflect.Ptr {
			continue
		}
		// Reply type must be exported.
		if !isExportedOrBuiltinType(replyType) {
			continue
		}
		// Method needs one out.
		if mtype.NumOut() != 1 {
			continue
		}
		// The return type of the method must be error.
		if returnType := mtype.Out(0); returnType != typeOfError {
			continue
		}

		// 封装方法信息
		mi := &MethodInfo{}
		mi.Name = method.Name

		if argType.Kind() == reflect.Ptr {
			argType = argType.Elem()
		}
		replyType = replyType.Elem()

		// 请求名
		mi.ReqName = argType.Name()
		mi.Req = generateTypeDefination(mi.ReqName, si.PkgPath, generateJSON(argType))
		// 回复名
		mi.ReplyName = replyType.Name()
		mi.Reply = generateTypeDefination(mi.ReplyName, si.PkgPath, generateJSON(replyType))

		si.Methods = append(si.Methods, mi)
	}

	if len(si.Methods) > 0 {
		r.Services[name] = si
	}

	return nil
}

// Unregister 解除注册服务
func (r *Reflection) Unregister(name string) error {
	delete(r.Services, name)
	return nil
}

func (r *Reflection) GetService(ctx context.Context, s string, reply *string) error {
	si, ok := r.Services[s]
	if !ok {
		return fmt.Errorf("not found service %s", s)
	}
	*reply = si.String()

	return nil
}

func (r *Reflection) GetServices(ctx context.Context, s string, reply *string) error {
	var buf bytes.Buffer

	pkg := `package `

	for _, si := range r.Services {
		if pkg == `package ` {
			pkg = pkg + si.PkgPath + "\n\n"
		}
		buf.WriteString(strings.ReplaceAll(si.String(), pkg, ""))
	}

	if pkg != `package ` {
		*reply = pkg + buf.String()
	} else {
		*reply = buf.String()
	}

	return nil
}

func generateTypeDefination(name, pkg string, jsonValue string) string {
	jsonValue = strings.TrimSpace(jsonValue)
	if jsonValue == "" || jsonValue == `""` {
		return ""
	}
	r := strings.NewReader(jsonValue)
	output, err := gojson.Generate(r, gojson.ParseJson, name, pkg, nil, false, true)
	if err != nil {
		log.Errorf("failed to generate json: %v", err)
		return ""
	}
	rt := strings.ReplaceAll(string(output), "``", "")
	return strings.ReplaceAll(rt, "package "+pkg+"\n\n", "")
}

func generateJSON(typ reflect.Type) string {
	v := reflect.New(typ).Interface()

	data, _ := json.Marshal(v)
	return string(data)
}

func isExported(name string) bool {
	rune, _ := utf8.DecodeRuneInString(name)
	return unicode.IsUpper(rune)
}

func isExportedOrBuiltinType(t reflect.Type) bool {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return isExported(t.Name()) || t.PkgPath() == ""
}
