use anyhow::Context as _;
use deno_core::v8;

const MAX_CAUSE_DEPTH: usize = 8;

/// Finds a stored host rejection through a bounded chain of native Error
/// causes.
///
/// Property reads on a rejected value can run user code. Only follow a `cause`
/// when its own descriptor exposes a data `value`, so accessor causes cannot
/// affect classification.
pub(super) fn find_rejection_index<T>(
    scope: &mut v8::PinScope<'_, '_>,
    rejection: v8::Local<v8::Value>,
    host_operation_rejections: &[(v8::Global<v8::Value>, T)],
) -> anyhow::Result<Option<usize>> {
    let cause_key = v8::String::new(scope, "cause")
        .context("Failed to create host operation error cause key")?;
    let value_key = v8::String::new(scope, "value")
        .context("Failed to create host operation error descriptor value key")?;
    let mut candidate = rejection;
    let mut visited = Vec::with_capacity(MAX_CAUSE_DEPTH);

    for _ in 0..MAX_CAUSE_DEPTH {
        if visited
            .iter()
            .any(|visited| candidate.strict_equals(v8::Local::new(scope, visited)))
        {
            break;
        }
        visited.push(v8::Global::new(scope, candidate));

        if let Some(index) = host_operation_rejections
            .iter()
            .position(|(stored_rejection, _)| {
                candidate.strict_equals(v8::Local::new(scope, stored_rejection))
            })
        {
            return Ok(Some(index));
        }

        if !candidate.is_native_error() {
            break;
        }
        let Some(candidate_object) = candidate.to_object(scope) else {
            break;
        };
        let Some(cause_descriptor) =
            candidate_object.get_own_property_descriptor(scope, cause_key.into())
        else {
            break;
        };
        let Ok(cause_descriptor) = v8::Local::<v8::Object>::try_from(cause_descriptor) else {
            break;
        };
        let Some(value_descriptor) =
            cause_descriptor.get_own_property_descriptor(scope, value_key.into())
        else {
            break;
        };
        let Ok(value_descriptor) = v8::Local::<v8::Object>::try_from(value_descriptor) else {
            break;
        };
        let Some(cause) = value_descriptor.get(scope, value_key.into()) else {
            break;
        };
        candidate = cause;
    }

    Ok(None)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{
        AtomicUsize,
        Ordering,
    };

    use deno_core::v8::{
        self,
        scope,
    };

    use super::find_rejection_index;

    static ACCESSOR_CAUSE_GETS: AtomicUsize = AtomicUsize::new(0);

    fn accessor_cause_getter<'s>(
        _scope: &mut v8::PinScope<'s, '_>,
        _args: v8::FunctionCallbackArguments<'s>,
        _return_value: v8::ReturnValue<v8::Value>,
    ) {
        ACCESSOR_CAUSE_GETS.fetch_add(1, Ordering::SeqCst);
    }

    macro_rules! with_v8_scope {
        ($scope:ident, $body:block) => {{
            crate::client::initialize_v8();
            let mut isolate = v8::Isolate::new(Default::default());
            scope!(let handle_scope, &mut isolate);
            let context = v8::Context::new(handle_scope, v8::ContextOptions::default());
            let $scope = &mut v8::ContextScope::new(handle_scope, context);
            $body
        }};
    }

    fn error<'s>(scope: &mut v8::PinScope<'s, '_>) -> v8::Local<'s, v8::Value> {
        let message = v8::String::new(scope, "host operation failed").unwrap();
        v8::Exception::error(scope, message)
    }

    fn set_cause(
        scope: &mut v8::PinScope<'_, '_>,
        error: v8::Local<v8::Value>,
        cause: v8::Local<v8::Value>,
    ) {
        let error = v8::Local::<v8::Object>::try_from(error).unwrap();
        let cause_key = v8::String::new(scope, "cause").unwrap();
        assert!(error.set(scope, cause_key.into(), cause).unwrap());
    }

    fn stored_rejection(
        scope: &mut v8::PinScope<'_, '_>,
        rejection: v8::Local<v8::Value>,
    ) -> Vec<(v8::Global<v8::Value>, ())> {
        vec![(v8::Global::new(scope, rejection), ())]
    }

    #[test]
    fn matches_a_direct_host_rejection() {
        with_v8_scope!(scope, {
            let rejection = error(scope);
            let stored = stored_rejection(scope, rejection);

            assert_eq!(
                find_rejection_index(scope, rejection, &stored).unwrap(),
                Some(0)
            );
        });
    }

    #[test]
    fn matches_a_native_error_with_a_host_rejection_cause() {
        with_v8_scope!(scope, {
            let rejection = error(scope);
            let wrapper = error(scope);
            set_cause(scope, wrapper, rejection);
            let stored = stored_rejection(scope, rejection);

            assert_eq!(
                find_rejection_index(scope, wrapper, &stored).unwrap(),
                Some(0)
            );
        });
    }

    #[test]
    fn ignores_a_native_error_without_a_cause() {
        with_v8_scope!(scope, {
            let rejection = error(scope);
            let wrapper = error(scope);
            let stored = stored_rejection(scope, rejection);

            assert_eq!(find_rejection_index(scope, wrapper, &stored).unwrap(), None);
        });
    }

    #[test]
    fn stops_at_a_cyclic_cause() {
        with_v8_scope!(scope, {
            let rejection = error(scope);
            let wrapper = error(scope);
            set_cause(scope, wrapper, wrapper);
            let stored = stored_rejection(scope, rejection);

            assert_eq!(find_rejection_index(scope, wrapper, &stored).unwrap(), None);
        });
    }

    #[test]
    fn does_not_invoke_an_accessor_cause() {
        with_v8_scope!(scope, {
            ACCESSOR_CAUSE_GETS.store(0, Ordering::SeqCst);
            let rejection = error(scope);
            let wrapper = error(scope);
            let wrapper = v8::Local::<v8::Object>::try_from(wrapper).unwrap();
            let cause_key = v8::String::new(scope, "cause").unwrap();
            let getter = v8::FunctionTemplate::new(scope, accessor_cause_getter)
                .get_function(scope)
                .unwrap();
            let descriptor = v8::PropertyDescriptor::new_from_get_set(
                getter.into(),
                v8::undefined(scope).into(),
            );
            assert!(wrapper
                .define_property(scope, cause_key.into(), &descriptor)
                .unwrap());
            let stored = stored_rejection(scope, rejection);

            assert_eq!(
                find_rejection_index(scope, wrapper.into(), &stored).unwrap(),
                None
            );
            assert_eq!(ACCESSOR_CAUSE_GETS.load(Ordering::SeqCst), 0);
        });
    }

    #[test]
    fn stops_before_a_cause_chain_longer_than_the_depth_limit() {
        with_v8_scope!(scope, {
            let rejection = error(scope);
            let mut wrapper = rejection;
            for _ in 0..8 {
                let next_wrapper = error(scope);
                set_cause(scope, next_wrapper, wrapper);
                wrapper = next_wrapper;
            }
            let stored = stored_rejection(scope, rejection);

            assert_eq!(find_rejection_index(scope, wrapper, &stored).unwrap(), None);
        });
    }
}
