package controller

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

type ManagedDatabase struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ManagedDatabaseSpec   `json:"spec,omitempty"`
	Status ManagedDatabaseStatus `json:"status,omitempty"`
}

type ManagedDatabaseList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items           []ManagedDatabase `json:"items"`
}

type ManagedDatabaseSpec struct {
	Engine string `json:"engine"`
	SizeGB int    `json:"sizeGB"`
}

type ManagedDatabaseStatus struct {
	ID       string `json:"id,omitempty"`
	State    string `json:"state,omitempty"`
	Endpoint string `json:"endpoint,omitempty"`
}

func (in *ManagedDatabase) DeepCopyObject() runtime.Object {
	out := &ManagedDatabase{}
	*out = *in
	out.ObjectMeta = *in.ObjectMeta.DeepCopy()
	return out
}

func (in *ManagedDatabaseList) DeepCopyObject() runtime.Object {
	out := &ManagedDatabaseList{}
	*out = *in
	out.ListMeta = *in.ListMeta.DeepCopy()
	if in.Items != nil {
		out.Items = make([]ManagedDatabase, len(in.Items))
		for i := range in.Items {
			out.Items[i] = *in.Items[i].DeepCopyObject().(*ManagedDatabase)
		}
	}
	return out
}
