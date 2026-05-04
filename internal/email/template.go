package email

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"text/template"
)

// ErrTemplateNotFound is returned when no template is registered for an EmailType.
// Permanent — DLQ.
var ErrTemplateNotFound = errors.New("email template not found")

// ErrTemplateRenderFailed is returned when template execution fails (missing
// required field, type mismatch, etc.). Permanent — DLQ.
var ErrTemplateRenderFailed = errors.New("email template render failed")

// renderedEmail is the output of template rendering.
type renderedEmail struct {
	Subject  string
	TextBody string
}

// Registry holds compiled templates keyed by EmailType and renders them
// against typed template-data structs decoded from email_messages.template_data.
type Registry struct {
	entries map[EmailType]*entry
}

type entry struct {
	subjectTmpl *template.Template
	bodyTmpl    *template.Template
	dataFactory func() any // returns a zero-value of the expected data struct
}

// NewRegistry returns a registry pre-loaded with all supported templates.
// Templates are intentionally minimal plain-text for Slice D — HTML design
// is out of scope.
func NewRegistry() *Registry {
	r := &Registry{entries: map[EmailType]*entry{}}
	r.mustRegister(TypeOrderConfirmation,
		`Pedido {{.OrderID}} confirmado`,
		`Olá {{.CustomerName}},

Seu pedido {{.OrderID}} foi confirmado em {{.OrderDate}}.

{{range .Items}}- {{.Name}} x{{.Quantity}} — {{.PriceFormatted}}
{{end}}
Total: {{.TotalFormatted}} {{.Currency}}

Obrigado por comprar na Blond Beauty!`,
		func() any { return &OrderConfirmationData{} },
	)
	r.mustRegister(TypePaymentApproved,
		`Pagamento aprovado — Pedido {{.OrderID}}`,
		`Seu pagamento de {{.AmountFormatted}} {{.Currency}} foi aprovado em {{.PaymentDate}}.

Pedido: {{.OrderID}}
Método: {{.PaymentMethod}}`,
		func() any { return &PaymentApprovedData{} },
	)
	r.mustRegister(TypePaymentFailed,
		`Problema com seu pagamento — Pedido {{.OrderID}}`,
		`Não foi possível processar seu pagamento para o pedido {{.OrderID}}.

Se desejar tentar novamente, acesse: {{.RetryURL}}`,
		func() any { return &PaymentFailedData{} },
	)
	r.mustRegister(TypeNFeAuthorized,
		`NF-e autorizada — Pedido {{.OrderID}}`,
		`A Nota Fiscal Eletrônica do pedido {{.OrderID}} foi autorizada.

Chave de acesso: {{.AccessKey}}

Para acessar o DANFE e o XML, utilize os links abaixo (válidos por tempo limitado):
DANFE: {{.DANFELink}}
XML:   {{.XMLLink}}`,
		func() any { return &NFeAuthorizedData{} },
	)
	r.mustRegister(TypePasswordReset,
		`Redefinição de senha`,
		`Para redefinir sua senha, acesse o link abaixo. Ele expira em breve.

{{.ResetURL}}

Se não foi você, ignore este e-mail.`,
		func() any { return &PasswordResetData{} },
	)
	r.mustRegister(TypeSecurityAlert,
		`Alerta de segurança: {{.AlertType}}`,
		`Foi detectada uma atividade suspeita na sua conta: {{.AlertType}}.

Detalhes: {{.Details}}

{{if .ActionURL}}Para tomar uma ação, acesse: {{.ActionURL}}{{end}}

Se reconhece esta atividade, nenhuma ação é necessária.`,
		func() any { return &SecurityAlertData{} },
	)
	r.mustRegister(TypeOrderShipped,
		`Pedido {{.OrderID}} enviado!`,
		`Seu pedido {{.OrderID}} foi enviado e está a caminho.

Transportadora: {{.Carrier}}
Código de rastreamento: {{.TrackingNumber}}
{{if .TrackingURL}}Rastreie sua entrega em: {{.TrackingURL}}{{end}}

Obrigado por comprar na Blond Beauty!`,
		func() any { return &OrderShippedData{} },
	)
	return r
}

func (r *Registry) mustRegister(t EmailType, subjectRaw, bodyRaw string, factory func() any) {
	subj := template.Must(template.New(string(t) + ".subj").Parse(subjectRaw))
	body := template.Must(template.New(string(t) + ".body").Parse(bodyRaw))
	r.entries[t] = &entry{subjectTmpl: subj, bodyTmpl: body, dataFactory: factory}
}

// Render decodes jsonData into the appropriate template-data struct and executes
// the template. Returns ErrTemplateNotFound or ErrTemplateRenderFailed (both
// permanent) on failure. NEVER logs jsonData — it may contain PII or tokens.
func (r *Registry) Render(emailType EmailType, jsonData json.RawMessage) (renderedEmail, error) {
	e, ok := r.entries[emailType]
	if !ok {
		return renderedEmail{}, fmt.Errorf("%w: %q", ErrTemplateNotFound, emailType)
	}

	data := e.dataFactory()
	if len(jsonData) > 0 {
		if err := json.Unmarshal(jsonData, data); err != nil {
			return renderedEmail{}, fmt.Errorf("%w: decode template data: %v", ErrTemplateRenderFailed, err)
		}
	}

	var subjBuf, bodyBuf bytes.Buffer
	if err := e.subjectTmpl.Execute(&subjBuf, data); err != nil {
		return renderedEmail{}, fmt.Errorf("%w: subject: %v", ErrTemplateRenderFailed, err)
	}
	if err := e.bodyTmpl.Execute(&bodyBuf, data); err != nil {
		return renderedEmail{}, fmt.Errorf("%w: body: %v", ErrTemplateRenderFailed, err)
	}
	return renderedEmail{Subject: subjBuf.String(), TextBody: bodyBuf.String()}, nil
}

// ---- template data structs -----------------------------------------------
// These are the expected shapes of email_messages.template_data per email type.
// The API must populate these fields when inserting the email_messages row.

type OrderItem struct {
	Name           string `json:"name"`
	Quantity       int    `json:"quantity"`
	PriceFormatted string `json:"price_formatted"`
}

type OrderConfirmationData struct {
	CustomerName   string      `json:"customer_name"`
	OrderID        string      `json:"order_id"`
	OrderDate      string      `json:"order_date"`
	Items          []OrderItem `json:"items"`
	TotalFormatted string      `json:"total_formatted"`
	Currency       string      `json:"currency"`
}

type PaymentApprovedData struct {
	OrderID         string `json:"order_id"`
	AmountFormatted string `json:"amount_formatted"`
	Currency        string `json:"currency"`
	PaymentMethod   string `json:"payment_method"`
	PaymentDate     string `json:"payment_date"`
}

type PaymentFailedData struct {
	OrderID  string `json:"order_id"`
	RetryURL string `json:"retry_url"`
}

// NFeAuthorizedData contains only short-lived, pre-authorized links generated
// by the API or storage layer. The Engine never generates or stores these URLs.
// Links must be treated as sensitive: never logged, never stored in logs or DLQ.
type NFeAuthorizedData struct {
	OrderID   string `json:"order_id"`
	AccessKey string `json:"access_key"` // NF-e access key (chave de acesso) — public
	DANFELink string `json:"danfe_link"` // short-lived signed URL — sensitive
	XMLLink   string `json:"xml_link"`   // short-lived signed URL — sensitive
}

// PasswordResetData carries only the short-lived URL generated by the API.
// The Engine never sees or stores the raw reset token — only this composed URL.
// SECURITY: this field MUST NOT be logged. The template body containing this
// URL must also not be logged. See service.go for enforcement.
type PasswordResetData struct {
	ResetURL string `json:"reset_url"` // SENSITIVE — never log
}

type SecurityAlertData struct {
	AlertType string `json:"alert_type"`
	Details   string `json:"details"`
	ActionURL string `json:"action_url,omitempty"`
}

// OrderShippedData is the template_data shape for TypeOrderShipped emails.
// Populated by the fulfillment worker after dispatch.
//
// SECURITY: TrackingURL is a carrier-generated link that may expose customer
// delivery address. It must not be logged, traced, or stored unencrypted outside
// the email_messages row. The Engine writes it only to email_messages.template_data
// (API-owned, access-controlled). It is intentionally omitted from all emitted
// message events.
type OrderShippedData struct {
	OrderID        string `json:"order_id"`
	Carrier        string `json:"carrier"`
	TrackingNumber string `json:"tracking_number"`
	TrackingURL    string `json:"tracking_url"` // SENSITIVE — never log
}
